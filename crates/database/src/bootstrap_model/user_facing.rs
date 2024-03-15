use std::{
    cmp,
    collections::BTreeMap,
};

use anyhow::Context;
use common::{
    document::{
        DeveloperDocument,
        ResolvedDocument,
    },
    index::IndexKeyBytes,
    interval::Interval,
    query::{
        CursorPosition,
        Order,
    },
    runtime::Runtime,
    types::{
        StableIndexName,
        WriteTimestamp,
    },
    version::Version,
};
use errors::ErrorMetadata;
use indexing::backend_in_memory_indexes::{
    BatchKey,
    RangeRequest,
};
use maplit::btreemap;
use value::{
    check_user_size,
    ConvexObject,
    DeveloperDocumentId,
    Size,
    TableName,
};

use crate::{
    metrics::{
        log_virtual_table_get,
        log_virtual_table_query,
    },
    transaction::MAX_PAGE_SIZE,
    unauthorized_error,
    virtual_tables::VirtualTable,
    PatchValue,
    TableModel,
    Transaction,
};

// Low-level model struct that represents a "user facing" data model
// on the database. This view differs from the authoritative system
// state in a few ways:
//
//  1. System tables are only visible for `Identity::Admin` or
//     `Identity::System`.
//  2. We support virtual tables.
//  3. The interface is in `DeveloperDocumentId`s, not `ResolvedDocumentId`.
//  4. We track user size limits for documents, which are more restrictive than
//     the database's limits.
//  5. We support branching on the `convex` NPM package's version.
pub struct UserFacingModel<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
}

impl<'a, RT: Runtime> UserFacingModel<'a, RT> {
    pub fn new(tx: &'a mut Transaction<RT>) -> Self {
        Self { tx }
    }

    #[convex_macro::instrument_future]
    pub async fn get(
        &mut self,
        id: DeveloperDocumentId,
        version: Option<Version>,
    ) -> anyhow::Result<Option<DeveloperDocument>> {
        Ok(self
            .get_with_ts(id, version)
            .await?
            .map(|(document, _)| document))
    }

    #[convex_macro::instrument_future]
    pub async fn get_with_ts(
        &mut self,
        id: DeveloperDocumentId,
        version: Option<Version>,
    ) -> anyhow::Result<Option<(DeveloperDocument, WriteTimestamp)>> {
        let mut batch_result = self
            .get_batch(btreemap! {
                0 => (id, version),
            })
            .await;
        batch_result
            .remove(&0)
            .context("get_batch missing batch key")?
    }

    /// Fetches a batch of documents by id.
    /// Stage 1: For each requested ID, set up the fetch, reading table and
    ///     index ids, checking virtual tables, computing index intervals,
    ///     and looking in the cache. In particular, cache hits for the
    ///     entire batch are based on the initial state.
    /// Stage 2: Execute all of the underlying fetches against persistence in
    ///     parallel.
    /// Stage 3: For each requested ID, add it to the cache and
    ///     usage records, and munge the index range's results into
    ///     DeveloperDocuments.
    ///
    /// This leads to completely deterministic results, down to usage counts
    /// and which requests hit the cache.
    /// Throughout the stages, each item in the batch is effectively separate,
    /// so their errors are calculated independently.
    /// Since stage 3 mutates common state in a loop, the items can affect each
    /// other, e.g. if one item overflows the transaction limits, the remainder
    /// of the batch will throw similar errors.
    /// TODO(lee) dedupe duplicate fetches within a batch, which requires
    /// cloning errors.
    #[convex_macro::instrument_future]
    pub async fn get_batch(
        &mut self,
        ids: BTreeMap<BatchKey, (DeveloperDocumentId, Option<Version>)>,
    ) -> BTreeMap<BatchKey, anyhow::Result<Option<(DeveloperDocument, WriteTimestamp)>>> {
        let mut results = BTreeMap::new();
        let mut ids_to_fetch = BTreeMap::new();
        let batch_size = ids.len();
        for (batch_key, (id, version)) in ids {
            let resolve_result: anyhow::Result<_> = try {
                if self.tx.virtual_table_mapping().number_exists(id.table()) {
                    // TODO(lee) batch virtual table gets
                    log_virtual_table_get();
                    let table_name = self.tx.virtual_table_mapping().name(*id.table())?;
                    match VirtualTable::new(self.tx).get(&id, version).await? {
                        Some(result) => {
                            self.tx.reads.record_read_document(
                                table_name,
                                result.0.size(),
                                &self.tx.usage_tracker,
                                true,
                            )?;
                            assert!(results.insert(batch_key, Ok(Some(result))).is_none());
                        },
                        None => {
                            assert!(results.insert(batch_key, Ok(None)).is_none());
                        },
                    }
                } else {
                    if !self.tx.table_mapping().table_number_exists()(*id.table()) {
                        assert!(results.insert(batch_key, Ok(None)).is_none());
                        continue;
                    }
                    let id_ = id.map_table(self.tx.table_mapping().inject_table_id())?;
                    let table_name = self.tx.table_mapping().tablet_name(id_.table().table_id)?;
                    ids_to_fetch.insert(batch_key, (id_, table_name));
                }
            };
            if let Err(e) = resolve_result {
                assert!(results.insert(batch_key, Err(e)).is_none());
            }
        }
        let fetched_results = self.tx.get_inner_batch(ids_to_fetch).await;
        for (batch_key, inner_result) in fetched_results {
            let result: anyhow::Result<_> = try {
                let developer_result = inner_result?.map(|(doc, ts)| (doc.to_developer(), ts));
                assert!(results.insert(batch_key, Ok(developer_result)).is_none());
            };
            if let Err(e) = result {
                assert!(results.insert(batch_key, Err(e)).is_none());
            }
        }
        assert_eq!(results.len(), batch_size);
        results
    }

    /// Creates a new document with given value in the specified table.
    #[convex_macro::instrument_future]
    pub async fn insert(
        &mut self,
        table: TableName,
        value: ConvexObject,
    ) -> anyhow::Result<DeveloperDocumentId> {
        if self.tx.virtual_system_mapping().is_virtual_table(&table) {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ReadOnlyTable",
                format!("{table} is a read-only table"),
            ));
        }

        check_user_size(value.size())?;
        self.tx.retention_validator.fail_if_falling_behind()?;
        let id = self.tx.id_generator.generate(&table);

        let creation_time = self.tx.next_creation_time.increment()?;

        if table.is_system() {
            anyhow::bail!(ErrorMetadata::bad_request(
                "InvalidTableName",
                format!("Invalid table name {table} starts with metadata prefix '_'")
            ));
        }

        // Note that the index and document store updates within `self.insert_document`
        // below are fallible, and since the layers above still have access to
        // the `Transaction` in that case (we only have `&mut self` here, not a
        // consuming `self`), we need to make sure we leave the transaction in a
        // consistent state on error.
        //
        // It's okay for us to insert the table write here and fail below: At worse the
        // transaction will contain an insertion for an empty table's `_tables`
        // record. On the other hand, it's not okay for us to succeed an
        // insertion into the index/document store and then fail to insert the
        // table metadata. If the user then subsequently commits that transaction,
        // they'll have a record that points to a nonexistent table.
        TableModel::new(self.tx)
            .insert_table_metadata(&table)
            .await?;
        let document = ResolvedDocument::new(
            id.clone()
                .map_table(self.tx.table_mapping().name_to_id_user_input())?,
            creation_time,
            value,
        )?;
        let document_id = self.tx.insert_document(document).await?;

        Ok(document_id.into())
    }

    /// Merges the existing document with the given object. Will overwrite any
    /// conflicting fields.
    #[convex_macro::instrument_future]
    pub async fn patch(
        &mut self,
        id: DeveloperDocumentId,
        value: PatchValue,
    ) -> anyhow::Result<DeveloperDocument> {
        if self.tx.is_system(*id.table())
            && !(self.tx.identity.is_admin() || self.tx.identity.is_system())
        {
            anyhow::bail!(unauthorized_error("patch"))
        }
        self.tx.retention_validator.fail_if_falling_behind()?;

        let id_ = id.map_table(self.tx.table_mapping().inject_table_id())?;

        let new_document = self.tx.patch_inner(id_, value).await?;

        // Check the size of the patched document.
        if !self.tx.is_system(*id.table()) {
            check_user_size(new_document.size())?;
        }

        let developer_document = new_document.to_developer();
        Ok(developer_document)
    }

    /// Replace the document with the given value.
    #[convex_macro::instrument_future]
    pub async fn replace(
        &mut self,
        id: DeveloperDocumentId,
        value: ConvexObject,
    ) -> anyhow::Result<DeveloperDocument> {
        if self.tx.is_system(*id.table())
            && !(self.tx.identity.is_admin() || self.tx.identity.is_system())
        {
            anyhow::bail!(unauthorized_error("replace"))
        }
        if !self.tx.is_system(*id.table()) {
            check_user_size(value.size())?;
        }
        self.tx.retention_validator.fail_if_falling_behind()?;
        let id_ = id.map_table(self.tx.table_mapping().inject_table_id())?;

        let new_document = self.tx.replace_inner(id_, value).await?;
        let developer_document = new_document.to_developer();
        Ok(developer_document)
    }

    /// Delete the document at the given path -- called from user facing APIs
    /// (e.g. syscalls)
    #[convex_macro::instrument_future]
    pub async fn delete(&mut self, id: DeveloperDocumentId) -> anyhow::Result<DeveloperDocument> {
        if self.tx.is_system(*id.table())
            && !(self.tx.identity.is_admin() || self.tx.identity.is_system())
        {
            anyhow::bail!(unauthorized_error("delete"))
        }
        self.tx.retention_validator.fail_if_falling_behind()?;

        let id_ = id.map_table(&self.tx.table_mapping().inject_table_id())?;
        let document = self.tx.delete_inner(id_).await?;
        Ok(document.to_developer())
    }

    pub fn record_read_document(
        &mut self,
        document: &DeveloperDocument,
        table_name: &TableName,
    ) -> anyhow::Result<()> {
        let is_virtual_table = self
            .tx
            .virtual_system_mapping()
            .is_virtual_table(table_name);
        self.tx.reads.record_read_document(
            table_name.clone(),
            document.size(),
            &self.tx.usage_tracker,
            is_virtual_table,
        )
    }

    /// NOTE: returns a page of results. Callers must call record_read_document
    /// for all documents returned from the index stream.
    #[convex_macro::instrument_future]
    pub async fn index_range(
        &mut self,
        stable_index_name: &StableIndexName,
        interval: &Interval,
        order: Order,
        mut max_rows: usize,
        version: Option<Version>,
    ) -> anyhow::Result<(
        Vec<(IndexKeyBytes, DeveloperDocument, WriteTimestamp)>,
        CursorPosition,
    )> {
        if interval.is_empty() {
            return Ok((vec![], CursorPosition::End));
        }

        max_rows = cmp::min(max_rows, MAX_PAGE_SIZE);

        let tablet_index_name = match stable_index_name {
            StableIndexName::Physical(tablet_index_name) => tablet_index_name,
            StableIndexName::Virtual(index_name, tablet_index_name) => {
                log_virtual_table_query();
                return VirtualTable::new(self.tx)
                    .index_range(
                        RangeRequest {
                            index_name: tablet_index_name.clone(),
                            printable_index_name: index_name.clone(),
                            interval: interval.clone(),
                            order,
                            max_size: max_rows,
                        },
                        version,
                    )
                    .await;
            },
            StableIndexName::Missing => {
                return Ok((vec![], CursorPosition::End));
            },
        };
        let index_name = tablet_index_name
            .clone()
            .map_table(&self.tx.table_mapping().tablet_to_name())?;

        let (results, cursor) = self
            .tx
            .index
            .range(
                &mut self.tx.reads,
                RangeRequest {
                    index_name: tablet_index_name.clone(),
                    printable_index_name: index_name,
                    interval: interval.clone(),
                    order,
                    max_size: max_rows,
                },
            )
            .await?;
        let developer_results = results
            .into_iter()
            .map(|(key, doc, ts)| {
                let doc = doc.to_developer();
                anyhow::Ok((key, doc, ts))
            })
            .try_collect()?;
        Ok((developer_results, cursor))
    }
}