use anyhow::Context;
use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentFunctionPath,
        ComponentId,
    },
    errors::JsError,
    knobs::FUNCTION_MAX_ARGS_SIZE,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    types::{
        AllowedVisibility,
        UdfType,
    },
    version::{
        Version,
        DEPRECATION_THRESHOLD,
    },
};
use database::{
    unauthorized_error,
    Transaction,
};
use errors::ErrorMetadata;
use humansize::{
    FormatSize,
    BINARY,
};
use keybroker::Identity;
use model::{
    backend_state::{
        types::BackendState,
        BackendStateModel,
        DISABLED_ERROR_MESSAGE,
        PAUSED_ERROR_MESSAGE,
    },
    modules::{
        module_versions::Visibility,
        ModuleModel,
    },
    udf_config::UdfConfigModel,
};
#[cfg(any(test, feature = "testing"))]
use proptest::arbitrary::Arbitrary;
#[cfg(any(test, feature = "testing"))]
use proptest::strategy::Strategy;
use serde_json::Value as JsonValue;
use value::{
    ConvexArray,
    ConvexValue,
    Size,
};

use crate::parse_udf_args;
pub async fn validate_schedule_args<RT: Runtime>(
    path: ComponentFunctionPath,
    udf_args: Vec<JsonValue>,
    scheduled_ts: UnixTimestamp,
    udf_ts: UnixTimestamp,
    tx: &mut Transaction<RT>,
) -> anyhow::Result<(ComponentFunctionPath, ConvexArray)> {
    // We validate the following mostly so the developer don't get the timestamp
    // wrong with more than order of magnitude.
    let delta = scheduled_ts.as_secs_f64() - udf_ts.as_secs_f64();
    if delta > 5.0 * 366.0 * 24.0 * 3600.0 {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidScheduledFunctionDelay",
            format!("{scheduled_ts:?} is more than 5 years in the future")
        ));
    }
    if delta < -5.0 * 366.0 * 24.0 * 3600.0 {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidScheduledFunctionDelay",
            format!("{scheduled_ts:?} is more than 5 years in the past")
        ));
    }

    // We do serialize the arguments, so this is likely our fault.
    let udf_args = parse_udf_args(&path, udf_args)?;

    // Even though we might use different version of modules when executing,
    // we do validate that the scheduled function exists at time of scheduling.
    // We do it here instead of within transaction in order to leverage the module
    // cache.
    let canonicalized = path.clone().canonicalize();
    let module = ModuleModel::new(tx)
        .get_metadata_for_function(canonicalized.clone())
        .await?
        .with_context(|| {
            let p = String::from(path.udf_path.module().clone());
            ErrorMetadata::bad_request(
                "InvalidScheduledFunction",
                format!("Attempted to schedule function at nonexistent path: {p}",),
            )
        })?;

    // We validate the function name if analyzed modules are available. Note
    // that scheduling was added after we started persisting the result
    // of analyze, we should always validate in practice. We will tighten
    // the interface and make AnalyzedResult non-optional in the future.
    let function_name = canonicalized.udf_path.function_name();
    if let Some(analyze_result) = &module.analyze_result {
        let found = analyze_result
            .functions
            .iter()
            .any(|f| &f.name == function_name);
        if !found {
            anyhow::bail!(ErrorMetadata::bad_request(
                "InvalidScheduledFunction",
                format!(
                    "Attempted to schedule function, but no exported function {} found in the \
                     file: {}. Did you forget to export it?",
                    function_name,
                    String::from(path.as_root_udf_path()?.module().clone()),
                ),
            ));
        }
    }

    Ok((path, udf_args))
}

fn missing_or_internal_error(path: &CanonicalizedComponentFunctionPath) -> anyhow::Result<String> {
    Ok(format!(
        "Could not find public function for '{}'. Did you forget to run `npx convex dev` or `npx \
         convex deploy`?",
        String::from(path.as_root_udf_path()?.clone().strip())
    ))
}

async fn udf_version<RT: Runtime>(
    path: &CanonicalizedComponentFunctionPath,
    tx: &mut Transaction<RT>,
) -> anyhow::Result<Result<Version, JsError>> {
    let udf_config = UdfConfigModel::new(tx).get().await?;

    let udf_version = match udf_config {
        Some(udf_config) if udf_config.server_version > DEPRECATION_THRESHOLD.npm.unsupported => {
            udf_config.server_version.clone()
        },
        _ => {
            if udf_config.is_none()
                && ModuleModel::new(tx)
                    .get_analyzed_function(path)
                    .await?
                    .is_err()
            {
                // We don't have a UDF config and we can't find the analyzed function.
                // Likely this developer has never pushed before, so give them
                // the missing error message.
                return Ok(Err(JsError::from_message(missing_or_internal_error(path)?)));
            }

            let unsupported = format!(
                "Convex functions at or below version {} are no longer supported. Update your \
                 Convex npm package and then push your functions again with `npx convex deploy` \
                 or `npx convex dev`.",
                DEPRECATION_THRESHOLD.npm.unsupported
            );

            return Ok(Err(JsError::from_message(unsupported)));
        },
    };
    Ok(Ok(udf_version))
}

/// The path and args to a UDF that have already undergone validation.
///
/// This validation includes:
/// - Checking the visibility of the UDF.
/// - Checking that the UDF is the correct type.
/// - Checking the args size.
/// - Checking that the args pass validation.
///
/// This should only be constructed via `ValidatedPathAndArgs::new` to use the
/// type system to enforce that validation is never skipped.
#[derive(Clone, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "testing"), derive(Debug))]
pub struct ValidatedPathAndArgs {
    path: CanonicalizedComponentFunctionPath,
    args: ConvexArray,
    // Not set for system modules.
    npm_version: Option<Version>,
}

#[cfg(any(test, feature = "testing"))]
impl Arbitrary for ValidatedPathAndArgs {
    type Parameters = ();

    type Strategy = impl Strategy<Value = ValidatedPathAndArgs>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;

        any::<(CanonicalizedComponentFunctionPath, ConvexArray)>().prop_map(|(path, args)| {
            ValidatedPathAndArgs {
                path,
                args,
                npm_version: None,
            }
        })
    }
}

impl ValidatedPathAndArgs {
    /// Check if the function being called matches the allowed visibility and
    /// return a ValidatedPathAndARgs or an appropriate JsError.
    ///
    /// We want to use the same error message for "this function exists, but
    /// with the wrong visibility" and "this function does not exist" so we
    /// don't leak which non-public functions exist.
    pub async fn new<RT: Runtime>(
        allowed_visibility: AllowedVisibility,
        tx: &mut Transaction<RT>,
        path: CanonicalizedComponentFunctionPath,
        args: ConvexArray,
        expected_udf_type: UdfType,
    ) -> anyhow::Result<Result<ValidatedPathAndArgs, JsError>> {
        if path.udf_path.is_system() {
            anyhow::ensure!(
                path.component.is_root(),
                "System module inside non-root component?"
            );

            // We don't analyze system modules, so we don't validate anything
            // except the identity for them.
            let result = if tx.identity().is_admin() || tx.identity().is_system() {
                Ok(ValidatedPathAndArgs {
                    path,
                    args,
                    npm_version: None,
                })
            } else {
                Err(JsError::from_message(
                    unauthorized_error("Executing function").to_string(),
                ))
            };
            return Ok(result);
        }

        let mut backend_state_model = BackendStateModel::new(tx);
        let backend_state = backend_state_model.get_backend_state().await?;
        match backend_state {
            BackendState::Running => {},
            BackendState::Paused => {
                return Ok(Err(JsError::from_message(PAUSED_ERROR_MESSAGE.to_string())));
            },
            BackendState::Disabled => {
                return Ok(Err(JsError::from_message(
                    DISABLED_ERROR_MESSAGE.to_string(),
                )));
            },
        }

        let udf_version = match udf_version(&path, tx).await? {
            Ok(udf_version) => udf_version,
            Err(e) => return Ok(Err(e)),
        };

        // AnalyzeResult result should be populated for all supported versions.
        let Ok(analyzed_function) = ModuleModel::new(tx).get_analyzed_function(&path).await? else {
            return Ok(Err(JsError::from_message(missing_or_internal_error(
                &path,
            )?)));
        };

        let identity = tx.identity();
        match identity {
            // This is an admin, so allow calling all functions
            Identity::InstanceAdmin(_) | Identity::ActingUser(..) => (),
            _ => match allowed_visibility {
                AllowedVisibility::All => (),
                AllowedVisibility::PublicOnly => match analyzed_function.visibility {
                    Some(Visibility::Public) => (),
                    Some(Visibility::Internal) => {
                        return Ok(Err(JsError::from_message(missing_or_internal_error(
                            &path,
                        )?)));
                    },
                    None => {
                        anyhow::bail!(
                            "No visibility found for analyzed function {}",
                            path.as_root_udf_path()?
                        );
                    },
                },
            },
        };
        if expected_udf_type != analyzed_function.udf_type {
            anyhow::ensure!(path.component.is_root());
            return Ok(Err(JsError::from_message(format!(
                "Trying to execute {} as {}, but it is defined as {}.",
                path.as_root_udf_path()?,
                expected_udf_type,
                analyzed_function.udf_type
            ))));
        }

        if args.size() > *FUNCTION_MAX_ARGS_SIZE {
            return Ok(Err(JsError::from_message(format!(
                "Arguments for {} are too large (actual: {}, limit: {})",
                String::from(path.as_root_udf_path()?.clone()),
                args.size().format_size(BINARY),
                (*FUNCTION_MAX_ARGS_SIZE).format_size(BINARY),
            ))));
        }

        // If the UDF has an args validator, check that these args match.
        let args_validation_error = analyzed_function.args.check_args(
            &args,
            &tx.table_mapping().clone(),
            &tx.virtual_table_mapping().clone(),
        )?;

        if let Some(error) = args_validation_error {
            return Ok(Err(JsError::from_message(format!(
                "ArgumentValidationError: {error}",
            ))));
        }

        Ok(Ok(ValidatedPathAndArgs {
            path,
            args,
            npm_version: Some(udf_version),
        }))
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_tests(
        path: CanonicalizedComponentFunctionPath,
        args: ConvexArray,
        npm_version: Option<Version>,
    ) -> Self {
        Self {
            path,
            args,
            npm_version,
        }
    }

    pub fn path(&self) -> &CanonicalizedComponentFunctionPath {
        &self.path
    }

    pub fn consume(
        self,
    ) -> (
        CanonicalizedComponentFunctionPath,
        ConvexArray,
        Option<Version>,
    ) {
        (self.path, self.args, self.npm_version)
    }

    pub fn npm_version(&self) -> &Option<Version> {
        &self.npm_version
    }

    pub fn from_proto(
        pb::common::ValidatedPathAndArgs {
            path,
            args,
            npm_version,
        }: pb::common::ValidatedPathAndArgs,
    ) -> anyhow::Result<Self> {
        let args_json: JsonValue =
            serde_json::from_slice(&args.ok_or_else(|| anyhow::anyhow!("Missing args"))?)?;
        let args_value = ConvexValue::try_from(args_json)?;
        let args = ConvexArray::try_from(args_value)?;
        Ok(Self {
            path: CanonicalizedComponentFunctionPath {
                component: ComponentId::Root,
                udf_path: path
                    .ok_or_else(|| anyhow::anyhow!("Missing udf_path"))?
                    .parse()?,
            },
            args,
            npm_version: npm_version.map(|v| Version::parse(&v)).transpose()?,
        })
    }
}

impl TryFrom<ValidatedPathAndArgs> for pb::common::ValidatedPathAndArgs {
    type Error = anyhow::Error;

    fn try_from(
        ValidatedPathAndArgs {
            path,
            args,
            npm_version,
        }: ValidatedPathAndArgs,
    ) -> anyhow::Result<Self> {
        let args_json = JsonValue::from(args);
        let args = serde_json::to_vec(&args_json)?;
        anyhow::ensure!(path.component.is_root());
        Ok(Self {
            path: Some(path.udf_path.to_string()),
            args: Some(args),
            npm_version: npm_version.map(|v| v.to_string()),
        })
    }
}

/// A UDF path that has been validated to be an HTTP route.
///
/// This should only be constructed via `ValidatedHttpRoute::try_from` to use
/// the type system to enforce that validation is never skipped.
pub struct ValidatedHttpPath {
    path: CanonicalizedComponentFunctionPath,
    npm_version: Option<Version>,
}

impl ValidatedHttpPath {
    #[cfg(any(test, feature = "testing"))]
    pub async fn new_for_tests<RT: Runtime>(
        tx: &mut Transaction<RT>,
        udf_path: sync_types::CanonicalizedUdfPath,
        npm_version: Option<Version>,
    ) -> anyhow::Result<Self> {
        if !udf_path.is_system() {
            BackendStateModel::new(tx)
                .fail_while_paused_or_disabled()
                .await?;
        }
        Ok(Self {
            path: CanonicalizedComponentFunctionPath {
                component: ComponentId::Root,
                udf_path,
            },
            npm_version,
        })
    }

    pub async fn new<RT: Runtime>(
        tx: &mut Transaction<RT>,
        path: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<Result<Self, JsError>> {
        // This is not a developer error on purpose.
        anyhow::ensure!(
            path.udf_path.module().as_str() == "http.js",
            "Unexpected http udf path: {:?}",
            path.udf_path,
        );
        if !path.udf_path.is_system() {
            BackendStateModel::new(tx)
                .fail_while_paused_or_disabled()
                .await?;
        }
        let udf_version = match udf_version(&path, tx).await? {
            Ok(udf_version) => udf_version,
            Err(e) => return Ok(Err(e)),
        };
        Ok(Ok(ValidatedHttpPath {
            path,
            npm_version: Some(udf_version),
        }))
    }

    pub fn npm_version(&self) -> &Option<Version> {
        &self.npm_version
    }

    pub fn path(&self) -> &CanonicalizedComponentFunctionPath {
        &self.path
    }
}

#[cfg(test)]
mod test {

    use proptest::prelude::*;

    use crate::ValidatedPathAndArgs;
    proptest! {
        #![proptest_config(
            ProptestConfig { failure_persistence: None, ..ProptestConfig::default() }
        )]

        #[test]
        fn test_udf_path_proto_roundtrip(v in any::<ValidatedPathAndArgs>()) {
            let proto = pb::common::ValidatedPathAndArgs::try_from(v.clone()).unwrap();
            let v2 = ValidatedPathAndArgs::from_proto(proto).unwrap();
            assert_eq!(v, v2);
        }
    }
}
