//! Provider lifecycle glue for LocalGateway.
//!
//! A provider supplies its business routes (or a custom adapter) plus a few
//! configuration values; this crate handles the rest:
//!
//! 1. binds `127.0.0.1:<port>` (`0` picks an ephemeral port);
//! 2. serves the business routes together with the built-in `GET` probe and
//!    `POST` notification endpoints;
//! 3. registers with `PUT /gw/services/{name}`, retrying with backoff while
//!    the gateway is still starting;
//! 4. refreshes the management base URL when the gateway announces
//!    `gateway_restarting` with `nextBaseUrl`;
//! 5. on Ctrl+C (or a caller-provided shutdown future) optionally calls
//!    `DELETE /gw/services/{name}` and then stops the HTTP server gracefully.
//!
//! The default `axum` feature adds the `axum::Router` adapter, `utoipa` /
//! `utoipa-axum` document collection, and the `run`/`run_until` entry points.
//! With `default-features = false` the lifecycle still works through
//! [`ProviderBuilder::run_with`] and a custom [`Adapter`].

mod adapter;
#[cfg(feature = "axum")]
mod axum_adapter;
mod builder;
mod endpoints;
mod error;
mod health;
mod openapi;

pub use adapter::{Adapter, AdapterContext, ShutdownSignal};
pub use builder::{ProviderBuilder, RegisterRetry};
pub use endpoints::{ControlEndpoints, ControlResponse};
pub use error::ProviderError;
pub use health::{DefaultHealthCheck, HealthCheck, HealthCheckFn, HealthFuture, HealthResponse};

/// Convenience re-export of the thin runtime SDK used for the management calls.
pub use localgateway_sdk as sdk;
pub use localgateway_sdk::{
    GatewayClient, GatewayClientBuilder, PatchOptions, RegisterOptions, SdkError,
};

#[cfg(feature = "axum")]
pub use axum_adapter::AxumAdapter;
#[cfg(feature = "axum")]
pub use utoipa;
#[cfg(feature = "axum")]
pub use utoipa_axum;
