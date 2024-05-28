pub mod definition;

use std::{
    collections::BTreeMap,
    sync::LazyLock,
};

use anyhow::Context;
use common::{
    bootstrap_model::components::{
        definition::ComponentDefinitionMetadata,
        ComponentMetadata,
        ComponentType,
    },
    components::{
        CanonicalizedComponentFunctionPath,
        CanonicalizedComponentModulePath,
        ComponentDefinitionId,
        ComponentDefinitionPath,
        ComponentId,
        ComponentName,
        ComponentPath,
    },
    document::{
        ParsedDocument,
        ResolvedDocument,
    },
    maybe_val,
    query::{
        IndexRange,
        IndexRangeExpression,
        Order,
        Query,
    },
    runtime::Runtime,
    types::IndexName,
};
use value::{
    FieldPath,
    InternalId,
    TableIdentifier,
    TableName,
    TableNamespace,
};

use crate::{
    defaults::{
        system_index,
        SystemIndex,
        SystemTable,
    },
    ResolvedQuery,
    Transaction,
    COMPONENT_DEFINITIONS_TABLE,
};

pub static COMPONENTS_TABLE: LazyLock<TableName> = LazyLock::new(|| {
    "_components"
        .parse()
        .expect("Invalid built-in _components table")
});

pub static COMPONENTS_BY_PARENT_INDEX: LazyLock<IndexName> =
    LazyLock::new(|| system_index(&COMPONENTS_TABLE, "by_parent_and_name"));
static PARENT_FIELD: LazyLock<FieldPath> = LazyLock::new(|| "parent".parse().unwrap());
static NAME_FIELD: LazyLock<FieldPath> = LazyLock::new(|| "name".parse().unwrap());

pub struct ComponentsTable;

impl SystemTable for ComponentsTable {
    fn table_name(&self) -> &'static TableName {
        &COMPONENTS_TABLE
    }

    fn indexes(&self) -> Vec<SystemIndex> {
        vec![SystemIndex {
            name: COMPONENTS_BY_PARENT_INDEX.clone(),
            fields: vec![PARENT_FIELD.clone(), NAME_FIELD.clone()]
                .try_into()
                .unwrap(),
        }]
    }

    fn validate_document(&self, document: ResolvedDocument) -> anyhow::Result<()> {
        ParsedDocument::<ComponentMetadata>::try_from(document)?;
        Ok(())
    }
}

pub struct BootstrapComponentsModel<'a, RT: Runtime> {
    pub tx: &'a mut Transaction<RT>,
}

impl<'a, RT: Runtime> BootstrapComponentsModel<'a, RT> {
    pub fn new(tx: &'a mut Transaction<RT>) -> Self {
        Self { tx }
    }

    pub async fn component_in_parent(
        &mut self,
        parent_and_name: Option<(InternalId, ComponentName)>,
    ) -> anyhow::Result<Option<ParsedDocument<ComponentMetadata>>> {
        let range = match parent_and_name {
            Some((parent, name)) => vec![
                IndexRangeExpression::Eq(PARENT_FIELD.clone(), maybe_val!(parent.to_string())),
                IndexRangeExpression::Eq(NAME_FIELD.clone(), maybe_val!(name.to_string())),
            ],
            None => vec![IndexRangeExpression::Eq(
                PARENT_FIELD.clone(),
                maybe_val!(null),
            )],
        };
        let mut query = ResolvedQuery::new(
            self.tx,
            TableNamespace::Global,
            Query::index_range(IndexRange {
                index_name: COMPONENTS_BY_PARENT_INDEX.clone(),
                range,
                order: Order::Asc,
            }),
        )?;
        let doc = query.next(self.tx, Some(1)).await?;
        doc.map(TryFrom::try_from).transpose()
    }

    pub async fn root_component(
        &mut self,
    ) -> anyhow::Result<Option<ParsedDocument<ComponentMetadata>>> {
        self.component_in_parent(None).await
    }

    pub async fn resolve_path(
        &mut self,
        path: ComponentPath,
    ) -> anyhow::Result<Option<ParsedDocument<ComponentMetadata>>> {
        let mut component_doc = match self.root_component().await? {
            Some(doc) => doc,
            None => return Ok(None),
        };
        for name in path.iter() {
            component_doc = match self
                .component_in_parent(Some((component_doc.id().internal_id(), name.clone())))
                .await?
            {
                Some(doc) => doc,
                None => return Ok(None),
            };
        }
        Ok(Some(component_doc))
    }

    pub async fn load_all_components(
        &mut self,
    ) -> anyhow::Result<Vec<ParsedDocument<ComponentMetadata>>> {
        let mut query = ResolvedQuery::new(
            self.tx,
            TableNamespace::Global,
            Query::full_table_scan(COMPONENTS_TABLE.clone(), Order::Asc),
        )?;
        let mut components = Vec::new();
        while let Some(doc) = query.next(self.tx, None).await? {
            components.push(doc.try_into()?);
        }
        Ok(components)
    }

    pub async fn get_component_path(
        &mut self,
        mut component_id: ComponentId,
    ) -> anyhow::Result<ComponentPath> {
        let mut path = Vec::new();
        let component_table = self
            .tx
            .table_mapping()
            .namespace(TableNamespace::Global)
            .id(&COMPONENTS_TABLE)?;
        while let ComponentId::Child(internal_id) = component_id {
            let component_doc: ParsedDocument<ComponentMetadata> = self
                .tx
                .get(component_table.id(internal_id))
                .await?
                .with_context(|| format!("component {internal_id} missing"))?
                .try_into()?;
            component_id = match &component_doc.component_type {
                ComponentType::App => ComponentId::Root,
                ComponentType::ChildComponent { parent, name, .. } => {
                    path.push(name.clone());
                    ComponentId::Child(*parent)
                },
            };
        }
        path.reverse();
        Ok(ComponentPath::from(path))
    }

    pub async fn component_definition(
        &mut self,
        component: ComponentId,
    ) -> anyhow::Result<ComponentDefinitionId> {
        let component_definition = match component {
            ComponentId::Root => ComponentDefinitionId::Root,
            ComponentId::Child(component_id) => {
                let component_table = self
                    .tx
                    .table_mapping()
                    .namespace(TableNamespace::Global)
                    .id(&COMPONENTS_TABLE)?;
                let component_doc: ParsedDocument<ComponentMetadata> = self
                    .tx
                    .get(component_table.id(component_id))
                    .await?
                    .context("component missing")?
                    .try_into()?;
                ComponentDefinitionId::Child(component_doc.definition_id)
            },
        };
        Ok(component_definition)
    }

    pub async fn load_component(
        &mut self,
        id: ComponentId,
    ) -> anyhow::Result<Option<ParsedDocument<ComponentMetadata>>> {
        let result = match id {
            ComponentId::Root => self.root_component().await?,
            ComponentId::Child(internal_id) => {
                let component_table = self
                    .tx
                    .table_mapping()
                    .namespace(TableNamespace::Global)
                    .id(&COMPONENTS_TABLE)?;
                self.tx
                    .get(component_table.id(internal_id))
                    .await?
                    .map(TryInto::try_into)
                    .transpose()?
            },
        };
        Ok(result)
    }

    pub async fn load_definition(
        &mut self,
        id: ComponentDefinitionId,
    ) -> anyhow::Result<ParsedDocument<ComponentDefinitionMetadata>> {
        let internal_id = match id {
            ComponentDefinitionId::Root => {
                let root_component = self
                    .root_component()
                    .await?
                    .context("Missing root component")?;
                root_component.definition_id
            },
            ComponentDefinitionId::Child(id) => id,
        };
        let component_definitions_table = self
            .tx
            .table_mapping()
            .namespace(TableNamespace::Global)
            .id(&COMPONENT_DEFINITIONS_TABLE)?;
        let doc: ParsedDocument<ComponentDefinitionMetadata> = self
            .tx
            .get(component_definitions_table.id(internal_id))
            .await?
            .context("Missing component definition")?
            .try_into()?;
        Ok(doc)
    }

    pub async fn load_all_definitions(
        &mut self,
    ) -> anyhow::Result<
        BTreeMap<ComponentDefinitionPath, ParsedDocument<ComponentDefinitionMetadata>>,
    > {
        let mut query = ResolvedQuery::new(
            self.tx,
            TableNamespace::Global,
            Query::full_table_scan(COMPONENT_DEFINITIONS_TABLE.clone(), Order::Asc),
        )?;
        let mut definitions = BTreeMap::new();
        while let Some(doc) = query.next(self.tx, None).await? {
            let definition: ParsedDocument<ComponentDefinitionMetadata> = doc.try_into()?;
            anyhow::ensure!(definitions
                .insert(definition.path.clone(), definition)
                .is_none());
        }
        Ok(definitions)
    }

    pub async fn function_path_to_module(
        &mut self,
        path: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<CanonicalizedComponentModulePath> {
        let definition_id = if path.component.is_root() {
            ComponentDefinitionId::Root
        } else {
            let component_metadata = self
                .resolve_path(path.component)
                .await?
                .context("Component not found")?;
            ComponentDefinitionId::Child(component_metadata.definition_id)
        };
        Ok(CanonicalizedComponentModulePath {
            component: definition_id,
            module_path: path.udf_path.module().clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common::{
        bootstrap_model::components::{
            definition::{
                ComponentDefinitionMetadata,
                ComponentDefinitionType,
                ComponentInstantiation,
            },
            ComponentMetadata,
            ComponentType,
        },
        components::{
            ComponentDefinitionPath,
            ComponentId,
            ComponentPath,
        },
    };
    use keybroker::Identity;
    use runtime::testing::TestRuntime;

    use super::definition::COMPONENT_DEFINITIONS_TABLE;
    use crate::{
        bootstrap_model::components::{
            BootstrapComponentsModel,
            COMPONENTS_TABLE,
        },
        test_helpers::new_test_database,
        SystemMetadataModel,
    };

    #[convex_macro::test_runtime]
    async fn test_component_path(rt: TestRuntime) -> anyhow::Result<()> {
        let db = new_test_database(rt.clone()).await;
        let mut tx = db.begin(Identity::system()).await?;
        let child_definition_path: ComponentDefinitionPath = "../app/child".parse().unwrap();
        let child_definition_id = SystemMetadataModel::new_global(&mut tx)
            .insert(
                &COMPONENT_DEFINITIONS_TABLE,
                ComponentDefinitionMetadata {
                    path: child_definition_path.clone(),
                    definition_type: ComponentDefinitionType::ChildComponent {
                        name: "child".parse().unwrap(),
                        args: BTreeMap::new(),
                    },
                    child_components: Vec::new(),
                    exports: BTreeMap::new(),
                }
                .try_into()?,
            )
            .await?;
        let root_definition_id = SystemMetadataModel::new_global(&mut tx)
            .insert(
                &COMPONENT_DEFINITIONS_TABLE,
                ComponentDefinitionMetadata {
                    path: "".parse().unwrap(),
                    definition_type: ComponentDefinitionType::App,
                    child_components: vec![ComponentInstantiation {
                        name: "child_subcomponent".parse().unwrap(),
                        path: child_definition_path,
                        args: BTreeMap::new(),
                    }],
                    exports: BTreeMap::new(),
                }
                .try_into()?,
            )
            .await?;
        let root_id = SystemMetadataModel::new_global(&mut tx)
            .insert(
                &COMPONENTS_TABLE,
                ComponentMetadata {
                    definition_id: root_definition_id.internal_id(),
                    component_type: ComponentType::App,
                }
                .try_into()?,
            )
            .await?;
        let child_id = SystemMetadataModel::new_global(&mut tx)
            .insert(
                &COMPONENTS_TABLE,
                ComponentMetadata {
                    definition_id: child_definition_id.internal_id(),
                    component_type: ComponentType::ChildComponent {
                        parent: root_id.internal_id(),
                        name: "subcomponent_child".parse()?,
                        args: Default::default(),
                    },
                }
                .try_into()?,
            )
            .await?;
        let resolved_path = BootstrapComponentsModel::new(&mut tx)
            .resolve_path(ComponentPath::from(vec!["subcomponent_child".parse()?]))
            .await?;
        assert_eq!(resolved_path.unwrap().id(), child_id);
        let path = BootstrapComponentsModel::new(&mut tx)
            .get_component_path(ComponentId::Child(child_id.internal_id()))
            .await?;
        assert_eq!(
            path,
            ComponentPath::from(vec!["subcomponent_child".parse()?]),
        );
        Ok(())
    }
}
