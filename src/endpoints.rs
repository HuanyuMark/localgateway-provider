//! Framework-neutral implementations of the built-in provider endpoints.

use std::sync::Arc;

use localgateway_sdk::GatewayClient;
use serde_json::{Value, json};

use crate::health::HealthCheck;

/// Response produced by a built-in endpoint.
///
/// Custom adapters translate the status code and JSON body to their own
/// response type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlResponse {
    /// HTTP status code.
    pub status: u16,
    /// JSON response body.
    pub body: Value,
}

/// State and handlers for the `GET` probe and the `POST` notification endpoint.
///
/// The axum adapter wires these into routes automatically. Custom adapters can
/// call [`Self::health`] and [`Self::handle_notification`] directly.
#[derive(Clone)]
pub struct ControlEndpoints {
    service: String,
    version: String,
    probe_path: String,
    notify_path: String,
    client: GatewayClient,
    health_check: Arc<dyn HealthCheck>,
}

impl ControlEndpoints {
    pub(crate) fn new(
        service: String,
        version: String,
        probe_path: String,
        notify_path: String,
        client: GatewayClient,
        health_check: Arc<dyn HealthCheck>,
    ) -> Self {
        Self {
            service,
            version,
            probe_path,
            notify_path,
            client,
            health_check,
        }
    }

    /// Service name reported at registration and from the probe endpoint.
    #[must_use]
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Provider version reported at registration and from the probe endpoint.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Path of the built-in probe endpoint (`/health` by default).
    #[must_use]
    pub fn probe_path(&self) -> &str {
        &self.probe_path
    }

    /// Path of the built-in gateway notification endpoint.
    #[must_use]
    pub fn notify_path(&self) -> &str {
        &self.notify_path
    }

    /// Response for the probe endpoint.
    ///
    /// The configured [`HealthCheck`] is awaited for every request. Without a
    /// custom check this is `200` with
    /// `{"status":"ok","service":...,"version":...}`.
    pub async fn health(&self) -> ControlResponse {
        let response = self.health_check.check().await;
        ControlResponse {
            status: response.status,
            body: response.body,
        }
    }

    /// Handle a `gateway_restarting` notification.
    ///
    /// The gateway posts
    /// `{"type":"gateway_restarting","nextBaseUrl":"http://127.0.0.1:<port>"}`
    /// to the notification endpoint before it shuts down. The `nextBaseUrl`
    /// value is applied to every clone of the management client, including the
    /// one used for registration and PATCH calls.
    #[must_use]
    pub fn handle_notification(&self, body: &[u8]) -> ControlResponse {
        let payload: Value = match serde_json::from_slice(body) {
            Ok(payload) => payload,
            Err(error) => return error_response(format!("invalid JSON body: {error}")),
        };

        let event = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if event != "gateway_restarting" {
            return ControlResponse {
                status: 200,
                body: json!({"status": "ignored", "type": event}),
            };
        }

        let next_base_url = payload
            .get("nextBaseUrl")
            .and_then(Value::as_str)
            .or_else(|| payload.get("next_base_url").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(next_base_url) = next_base_url else {
            return error_response(
                "gateway_restarting requires a non-empty nextBaseUrl".to_owned(),
            );
        };

        match self.client.set_base_url(next_base_url) {
            Ok(()) => ControlResponse {
                status: 200,
                body: json!({"status": "ok", "nextBaseUrl": next_base_url}),
            },
            Err(error) => error_response(format!("invalid nextBaseUrl: {error}")),
        }
    }
}

fn error_response(message: String) -> ControlResponse {
    ControlResponse {
        status: 400,
        body: json!({"status": "error", "message": message}),
    }
}
