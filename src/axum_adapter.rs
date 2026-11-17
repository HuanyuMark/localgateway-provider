//! Default `axum` adapter.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use crate::adapter::{Adapter, AdapterContext};
use crate::endpoints::{ControlEndpoints, ControlResponse};
use crate::error::ProviderError;

/// Serves a business [`axum::Router`] plus the built-in endpoints.
#[derive(Debug)]
pub struct AxumAdapter {
    router: Router,
}

impl AxumAdapter {
    pub(crate) fn new(router: Router) -> Self {
        Self { router }
    }
}

impl Adapter for AxumAdapter {
    async fn serve(self, context: AdapterContext) -> Result<(), ProviderError> {
        let probe_path = context.endpoints.probe_path().to_owned();
        let notify_path = context.endpoints.notify_path().to_owned();
        let builtins = Router::new()
            .route(&probe_path, get(probe_handler))
            .route(&notify_path, post(notify_handler))
            .with_state(context.endpoints);

        let app = merge_routers(self.router, builtins)?;
        let mut shutdown = context.shutdown;
        axum::serve(context.listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .map_err(|error| ProviderError::Server(format!("axum server failed: {error}")))
    }
}

async fn probe_handler(State(endpoints): State<ControlEndpoints>) -> Response {
    into_response(endpoints.health().await)
}

async fn notify_handler(State(endpoints): State<ControlEndpoints>, body: Bytes) -> Response {
    into_response(endpoints.handle_notification(&body))
}

fn into_response(response: ControlResponse) -> Response {
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, axum::Json(response.body)).into_response()
}

/// Merge the business router with the built-in routes.
///
/// `axum::Router` does not expose a route table, so a same-method overlap makes
/// `merge` panic ("Overlapping method route"). Catch that panic and surface it
/// as a clean startup error instead of letting it escape the task.
fn merge_routers(business: Router, builtins: Router) -> Result<Router, ProviderError> {
    catch_unwind(AssertUnwindSafe(|| business.merge(builtins))).map_err(|payload| {
        ProviderError::RouteConflict(format!(
            "the business axum router clashes with a built-in endpoint ({}); \
             change probe_path/notify_path or remove the conflicting business route",
            panic_message(payload.as_ref())
        ))
    })
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "axum rejected the merged router".to_owned()
    }
}
