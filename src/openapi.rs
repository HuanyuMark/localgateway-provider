//! OpenAPI document assembly for the registered provider.
//!
//! The library always reports a document, even when the provider supplies no
//! routes or spec. A missing document becomes a minimal OpenAPI 3.1 object with
//! `info.title`/`info.version` and the built-in probe operation. Raw
//! `serde_json` documents are normalized in place so they stay usable when the
//! caller already has an OpenAPI 3.0/3.1 file.

use serde_json::{Map, Value, json};

use crate::error::ProviderError;

const HEALTH_SUMMARY: &str = "LocalGateway health probe";
const HEALTH_OPERATION_ID: &str = "localgateway_health";

/// Default document used when neither utoipa nor raw JSON is supplied.
#[cfg(not(feature = "axum"))]
pub(crate) fn minimal_api_info(service: &str, version: &str, probe_path: &str) -> Value {
    let mut paths = Map::new();
    paths.insert(
        probe_path.to_owned(),
        json!({"get": health_operation_json()}),
    );
    json!({
        "openapi": "3.1.0",
        "info": {"title": service, "version": version},
        "paths": Value::Object(paths),
    })
}

/// Fill `info.title`, `info.version` and the probe operation into a raw JSON
/// document, preserving everything else.
pub(crate) fn normalize_api_info(
    mut value: Value,
    service: &str,
    version: &str,
    probe_path: &str,
) -> Result<Value, ProviderError> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| ProviderError::InvalidConfig("api_info must be a JSON object".to_owned()))?;

    let openapi = object
        .entry("openapi")
        .or_insert_with(|| Value::String("3.1.0".to_owned()));
    if !openapi.as_str().is_some_and(|value| !value.is_empty()) {
        *openapi = Value::String("3.1.0".to_owned());
    }

    let info = object.entry("info").or_insert_with(|| json!({}));
    let info = info.as_object_mut().ok_or_else(|| {
        ProviderError::InvalidConfig("api_info.info must be a JSON object".to_owned())
    })?;
    fill_string(info, "title", service);
    fill_string(info, "version", version);

    let paths = object.entry("paths").or_insert_with(|| json!({}));
    let paths = paths.as_object_mut().ok_or_else(|| {
        ProviderError::InvalidConfig("api_info.paths must be a JSON object".to_owned())
    })?;
    let probe = paths
        .entry(probe_path.to_owned())
        .or_insert_with(|| json!({}));
    let probe = probe.as_object_mut().ok_or_else(|| {
        ProviderError::InvalidConfig(format!(
            "api_info.paths[{probe_path:?}] must be a JSON object"
        ))
    })?;
    probe.entry("get").or_insert_with(health_operation_json);

    Ok(value)
}

fn fill_string(map: &mut Map<String, Value>, key: &str, fallback: &str) {
    let missing = map
        .get(key)
        .and_then(Value::as_str)
        .is_none_or(str::is_empty);
    if missing {
        map.insert(key.to_owned(), Value::String(fallback.to_owned()));
    }
}

fn health_operation_json() -> Value {
    json!({
        "summary": HEALTH_SUMMARY,
        "operationId": HEALTH_OPERATION_ID,
        "responses": {
            "200": {
                "description": "Provider is healthy",
                "content": {
                    "application/json": {"schema": {"type": "object"}}
                }
            }
        }
    })
}

/// Assemble a typed document from utoipa.
///
/// `overwrite_info` is used for documents collected by `OpenApiRouter`: their
/// default info comes from utoipa-axum's own crate metadata, so the provider's
/// service name and version replace it. Explicit documents only get missing
/// fields filled.
#[cfg(feature = "axum")]
pub(crate) fn resolve_typed_openapi(
    explicit: Option<utoipa::openapi::OpenApi>,
    collected: Option<utoipa::openapi::OpenApi>,
    service: &str,
    version: &str,
    probe_path: &str,
) -> Result<Value, ProviderError> {
    use utoipa::openapi::{Info, OpenApi, Paths};

    let (mut document, overwrite_info) = match (explicit, collected) {
        (Some(document), _) => (document, false),
        (None, Some(document)) => (document, true),
        (None, None) => (
            OpenApi::new(Info::new(service, version), Paths::new()),
            false,
        ),
    };

    if overwrite_info || document.info.title.trim().is_empty() {
        document.info.title = service.to_owned();
    }
    if overwrite_info || document.info.version.trim().is_empty() {
        document.info.version = version.to_owned();
    }
    ensure_probe_operation(&mut document, probe_path);

    serde_json::to_value(document).map_err(|error| {
        ProviderError::InvalidConfig(format!("failed to serialize the OpenAPI document: {error}"))
    })
}

#[cfg(feature = "axum")]
fn ensure_probe_operation(document: &mut utoipa::openapi::OpenApi, probe_path: &str) {
    let item = document
        .paths
        .paths
        .entry(probe_path.to_owned())
        .or_default();
    if item.get.is_none() {
        item.get = Some(health_operation());
    }
}

#[cfg(feature = "axum")]
fn health_operation() -> utoipa::openapi::path::Operation {
    use utoipa::openapi::path::Operation;
    use utoipa::openapi::response::{Response, Responses};

    let mut responses = Responses::new();
    responses.responses.insert(
        "200".to_owned(),
        Response::new("Provider is healthy").into(),
    );

    Operation::builder()
        .summary(Some(HEALTH_SUMMARY))
        .operation_id(Some(HEALTH_OPERATION_ID))
        .responses(responses)
        .build()
}
