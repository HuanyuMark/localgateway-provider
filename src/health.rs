//! Framework-neutral provider health checks.
//!
//! The built-in probe endpoint serves whatever a [`HealthCheck`] returns. The
//! default implementation keeps the historical always-healthy response; real
//! providers can inject a check that reflects business state, for example
//! returning `503` while a backing dependency is unavailable.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

/// Response produced by a [`HealthCheck`].
///
/// The status code and JSON body are passed through unchanged by the built-in
/// endpoints and the default axum adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthResponse {
    /// HTTP status code.
    pub status: u16,
    /// JSON response body.
    pub body: Value,
}

impl HealthResponse {
    /// Report an arbitrary status code and JSON body.
    #[must_use]
    pub fn new(status: u16, body: Value) -> Self {
        Self { status, body }
    }

    /// The default healthy response: `200` with
    /// `{"status":"ok","service":...,"version":...}`.
    #[must_use]
    pub fn healthy(service: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: json!({
                "status": "ok",
                "service": service.into(),
                "version": version.into(),
            }),
        }
    }
}

/// Boxed future returned by [`HealthCheck::check`].
///
/// Boxing keeps the trait dyn-compatible so custom checks can be injected as
/// `Arc<dyn HealthCheck>`.
pub type HealthFuture<'a> = Pin<Box<dyn Future<Output = HealthResponse> + Send + 'a>>;

/// Business-health probe served by `GET <probe_path>`.
///
/// The check is awaited on every probe request, so it may perform asynchronous
/// work (I/O, locks, timeouts). The gateway's probe timeout bounds how long a
/// single check may take.
///
/// Implementations are usually injected through
/// [`ProviderBuilder::health_check`](crate::ProviderBuilder::health_check). For
/// simple cases [`HealthCheckFn`] adapts an async closure.
pub trait HealthCheck: Send + Sync + 'static {
    /// Evaluate the provider's current health.
    fn check(&self) -> HealthFuture<'_>;
}

/// Default check used when none is supplied: always report `200` and the
/// registered service name and version.
#[derive(Debug, Clone)]
pub struct DefaultHealthCheck {
    service: String,
    version: String,
}

impl DefaultHealthCheck {
    /// Build the default always-healthy check.
    #[must_use]
    pub fn new(service: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            version: version.into(),
        }
    }
}

impl HealthCheck for DefaultHealthCheck {
    fn check(&self) -> HealthFuture<'_> {
        Box::pin(std::future::ready(HealthResponse::healthy(
            self.service.clone(),
            self.version.clone(),
        )))
    }
}

/// Adapter that turns an async closure into a [`HealthCheck`].
///
/// ```no_run
/// use std::sync::Arc;
/// use localgateway_provider::{HealthCheckFn, HealthResponse, ProviderBuilder};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let check = Arc::new(HealthCheckFn::new(|| async {
///     HealthResponse::new(503, serde_json::json!({"status": "degraded"}))
/// }));
///
/// ProviderBuilder::new("voicecenter", "0.1.0")
///     .health_check(check)
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct HealthCheckFn<F>(F);

impl<F> HealthCheckFn<F> {
    /// Wrap `check`, which must be callable repeatedly and return a `Send`
    /// future.
    #[must_use]
    pub fn new(check: F) -> Self {
        Self(check)
    }
}

impl<F, Fut> HealthCheck for HealthCheckFn<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HealthResponse> + Send + 'static,
{
    fn check(&self) -> HealthFuture<'_> {
        Box::pin((self.0)())
    }
}

impl<F> std::fmt::Debug for HealthCheckFn<F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HealthCheckFn")
            .finish_non_exhaustive()
    }
}
