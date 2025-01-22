use common::{
    components::ComponentId,
    types::BackendState,
};
use errors::ErrorMetadataAnyhowExt;
use futures::stream;
use keybroker::Identity;
use model::backend_state::BackendStateModel;
use runtime::testing::TestRuntime;

use crate::{
    test_helpers::ApplicationTestExt,
    Application,
};

#[convex_macro::test_runtime]
pub(crate) async fn test_backend_not_running_cannot_store_file(
    rt: TestRuntime,
) -> anyhow::Result<()> {
    let app = Application::new_for_tests(&rt).await?;

    let file_body = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![55; 1024 + 1]))
    }));
    let ok_result = app
        .store_file(ComponentId::Root, None, None, None, file_body)
        .await;
    assert!(ok_result.is_ok());

    let mut tx = app.begin(Identity::system()).await?;
    BackendStateModel::new(&mut tx)
        .toggle_backend_state(BackendState::Disabled)
        .await?;
    app.commit_test(tx).await?;
    let file_body = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![55; 1024 + 1]))
    }));
    let result = app
        .store_file(ComponentId::Root, None, None, None, file_body)
        .await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.is_bad_request());
    assert_eq!(error.short_msg(), "BackendIsNotRunning");
    Ok(())
}

#[convex_macro::test_runtime]
pub(crate) async fn test_store_file(rt: TestRuntime) -> anyhow::Result<()> {
    let app = Application::new_for_tests(&rt).await?;

    let file_body = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![42; 2048]))
    }));
    let result = app
        .store_file(ComponentId::Root, None, None, None, file_body)
        .await;
    assert!(result.is_ok());

    let file_body = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![42; 0]))
    }));
    let result = app
        .store_file(ComponentId::Root, None, None, None, file_body)
        .await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.is_bad_request());
    assert_eq!(error.short_msg(), "EmptyFile");

    Ok(())
}

#[convex_macro::test_runtime]
pub(crate) async fn test_delete_file(rt: TestRuntime) -> anyhow::Result<()> {
    let app = Application::new_for_tests(&rt).await?;

    let file_body = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![42; 2048]))
    }));
    let file_id = app
        .store_file(ComponentId::Root, None, None, None, file_body)
        .await?
        .file_id;

    let result = app.delete_file(file_id).await;
    assert!(result.is_ok());

    let result = app.delete_file(file_id).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.is_not_found());
    assert_eq!(error.short_msg(), "FileNotFound");

    Ok(())
}

#[convex_macro::test_runtime]
pub(crate) async fn test_list_files(rt: TestRuntime) -> anyhow::Result<()> {
    let app = Application::new_for_tests(&rt).await?;

    let file_body1 = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![42; 2048]))
    }));
    let file_id1 = app
        .store_file(ComponentId::Root, None, None, None, file_body1)
        .await?
        .file_id;

    let file_body2 = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![43; 2048]))
    }));
    let file_id2 = app
        .store_file(ComponentId::Root, None, None, None, file_body2)
        .await?
        .file_id;

    let files = app.list_files().await?;
    assert_eq!(files.len(), 2);
    assert!(files.iter().any(|file| file.file_id == file_id1));
    assert!(files.iter().any(|file| file.file_id == file_id2));

    Ok(())
}

#[convex_macro::test_runtime]
pub(crate) async fn test_get_file(rt: TestRuntime) -> anyhow::Result<()> {
    let app = Application::new_for_tests(&rt).await?;

    let file_body = Box::pin(stream::once(async {
        Ok(bytes::Bytes::from(vec![42; 2048]))
    }));
    let file_id = app
        .store_file(ComponentId::Root, None, None, None, file_body)
        .await?
        .file_id;

    let file = app.get_file(file_id).await?;
    assert_eq!(file.file_id, file_id);

    let result = app.get_file(file_id).await;
    assert!(result.is_ok());

    let non_existent_file_id = "non_existent_file_id".to_string();
    let result = app.get_file(non_existent_file_id).await;
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.is_not_found());
    assert_eq!(error.short_msg(), "FileNotFound");

    Ok(())
}
