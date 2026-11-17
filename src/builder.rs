//! Builder and lifecycle implementation.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use localgateway_sdk::api::models::RegistrationResponse;
use localgateway_sdk::{GatewayClient, RegisterOptions, SdkError};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::adapter::{Adapter, AdapterContext, ShutdownSignal};
use crate::endpoints::ControlEndpoints;
use crate::error::ProviderError;
use crate::health::{DefaultHealthCheck, HealthCheck};
use crate::openapi;

const MIN_PROBE_TIMEOUT_MS: u32 = 100;
const MAX_PROBE_TIMEOUT_MS: u32 = 60_000;
const DEFAULT_PROBE_PATH: &str = "/health";
const DEFAULT_NOTIFY_PATH: &str = "/_localgateway/events";
const UNREGISTER_TIMEOUT: Duration = Duration::from_secs(2);

/// Backoff policy for `PUT /gw/services/{name}` while the gateway is down.
///
/// The default retries for about 30 seconds, which lets a provider start
/// before the gateway. Registration stops early on validation errors, auth
/// errors, and 4xx responses that the gateway will not fix by waiting.
#[derive(Debug, Clone)]
pub struct RegisterRetry {
    /// Delay before the second attempt.
    pub initial_delay: Duration,
    /// Upper bound for the exponential delay.
    pub max_delay: Duration,
    /// Total time budget for registration.
    pub window: Duration,
}

impl Default for RegisterRetry {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(2),
            window: Duration::from_secs(30),
        }
    }
}

impl RegisterRetry {
    /// Build a custom backoff policy.
    #[must_use]
    pub fn new(initial_delay: Duration, max_delay: Duration, window: Duration) -> Self {
        Self {
            initial_delay,
            max_delay,
            window,
        }
    }
}

/// Configure and run a LocalGateway provider.
///
/// ```no_run
/// use axum::{Router, routing::get};
/// use localgateway_provider::ProviderBuilder;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let routes = Router::new().route("/api/hello", get(|| async { "hello" }));
/// ProviderBuilder::new("voicecenter", "0.1.0")
///     .port(47821)
///     .project_root("C:\\work\\voicecenter")
///     .routes(routes)
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ProviderBuilder {
    service: String,
    version: String,
    port: u16,
    probe_path: String,
    probe_timeout_ms: Option<u32>,
    project_root: Option<String>,
    api_info: Option<Value>,
    notify_path: String,
    unregister_on_shutdown: bool,
    gateway_client: Option<GatewayClient>,
    register_retry: RegisterRetry,
    health_check: Option<Arc<dyn HealthCheck>>,
    #[cfg(feature = "axum")]
    router: Option<axum::Router>,
    #[cfg(feature = "axum")]
    collected_openapi: Option<utoipa::openapi::OpenApi>,
    #[cfg(feature = "axum")]
    explicit_openapi: Option<utoipa::openapi::OpenApi>,
    #[cfg(feature = "axum")]
    business_paths: Vec<String>,
}

impl ProviderBuilder {
    /// Start configuring a provider.
    ///
    /// `service` is the registration name, `version` is reported in the
    /// registration body and in the generated OpenAPI `info.version`.
    #[must_use]
    pub fn new(service: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            version: version.into(),
            port: 0,
            probe_path: DEFAULT_PROBE_PATH.to_owned(),
            probe_timeout_ms: None,
            project_root: None,
            api_info: None,
            notify_path: DEFAULT_NOTIFY_PATH.to_owned(),
            unregister_on_shutdown: true,
            gateway_client: None,
            register_retry: RegisterRetry::default(),
            health_check: None,
            #[cfg(feature = "axum")]
            router: None,
            #[cfg(feature = "axum")]
            collected_openapi: None,
            #[cfg(feature = "axum")]
            explicit_openapi: None,
            #[cfg(feature = "axum")]
            business_paths: Vec::new(),
        }
    }

    /// Loopback port; `0` (the default) selects an ephemeral port.
    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Probe path served by the built-in endpoint (`/health` by default).
    #[must_use]
    pub fn probe_path(mut self, probe_path: impl Into<String>) -> Self {
        self.probe_path = probe_path.into();
        self
    }

    /// Probe timeout reported to the gateway, in milliseconds.
    #[must_use]
    pub fn probe_timeout_ms(mut self, probe_timeout_ms: u32) -> Self {
        self.probe_timeout_ms = Some(probe_timeout_ms);
        self
    }

    /// Project root reported at registration (used by client code generation).
    #[must_use]
    pub fn project_root(mut self, project_root: impl Into<String>) -> Self {
        self.project_root = Some(project_root.into());
        self
    }

    /// Raw OpenAPI 3.0/3.1 JSON document.
    ///
    /// This overrides a document collected from `OpenApiRouter` or passed to
    /// [`Self::openapi`]. Missing `info.title`, `info.version`, and the probe
    /// operation are filled in.
    #[must_use]
    pub fn api_info(mut self, api_info: Value) -> Self {
        self.api_info = Some(api_info);
        self
    }

    /// Gateway notification path (`/_localgateway/events` by default).
    #[must_use]
    pub fn notify_path(mut self, notify_path: impl Into<String>) -> Self {
        self.notify_path = notify_path.into();
        self
    }

    /// Call `DELETE /gw/services/{name}` on shutdown. Defaults to `true`.
    #[must_use]
    pub fn unregister_on_shutdown(mut self, unregister_on_shutdown: bool) -> Self {
        self.unregister_on_shutdown = unregister_on_shutdown;
        self
    }

    /// Use a preconfigured management client. Defaults to
    /// [`GatewayClient::new`], which reads the registry port.
    #[must_use]
    pub fn gateway_client(mut self, gateway_client: GatewayClient) -> Self {
        self.gateway_client = Some(gateway_client);
        self
    }

    /// Override the registration backoff policy.
    #[must_use]
    pub fn register_retry(mut self, register_retry: RegisterRetry) -> Self {
        self.register_retry = register_retry;
        self
    }

    /// Serve custom business health from the built-in probe endpoint.
    ///
    /// Without this the probe always reports `200` with the service name and
    /// version. A custom [`HealthCheck`] is awaited on every probe request, so
    /// it may return `503` (or another status) while the provider is still
    /// registered; the gateway then marks it unhealthy based on its probe
    /// policy. Use [`HealthCheckFn`](crate::HealthCheckFn) to adapt a closure.
    #[must_use]
    pub fn health_check(mut self, health_check: Arc<dyn HealthCheck>) -> Self {
        self.health_check = Some(health_check);
        self
    }

    /// Serve business routes through the default axum adapter.
    ///
    /// The built-in probe and notification routes are merged with this router.
    /// A same-method conflict is reported as [`ProviderError::RouteConflict`]
    /// before the provider starts accepting requests.
    #[cfg(feature = "axum")]
    #[must_use]
    pub fn routes(mut self, router: axum::Router) -> Self {
        self.router = Some(router);
        self.business_paths.clear();
        self
    }

    /// Collect business routes and the OpenAPI document from `OpenApiRouter`.
    ///
    /// `split_for_parts()` is called immediately; the probe operation and any
    /// missing `info` fields are added when the provider starts.
    #[cfg(feature = "axum")]
    #[must_use]
    pub fn openapi_router(mut self, router: utoipa_axum::router::OpenApiRouter) -> Self {
        let business_paths = router.get_openapi().paths.paths.keys().cloned().collect();
        let (router, openapi) = router.split_for_parts();
        self.router = Some(router);
        self.collected_openapi = Some(openapi);
        self.business_paths = business_paths;
        self
    }

    /// Use an explicitly assembled utoipa document.
    ///
    /// Empty `info.title`/`info.version` values are filled from the service and
    /// version; existing values are preserved. Raw [`Self::api_info`] still
    /// takes precedence.
    #[cfg(feature = "axum")]
    #[must_use]
    pub fn openapi(mut self, openapi: utoipa::openapi::OpenApi) -> Self {
        self.explicit_openapi = Some(openapi);
        self
    }

    /// Run with Ctrl+C as the shutdown signal.
    ///
    /// Requires a Tokio runtime with signal support (the default
    /// `#[tokio::main]` multi-thread runtime satisfies this).
    #[cfg(feature = "axum")]
    pub async fn run(self) -> Result<(), ProviderError> {
        self.run_until(std::future::pending()).await
    }

    /// Run with Ctrl+C or `shutdown` completing as the shutdown signal.
    #[cfg(feature = "axum")]
    pub async fn run_until<F>(mut self, shutdown: F) -> Result<(), ProviderError>
    where
        F: Future<Output = ()> + Send,
    {
        let adapter = crate::AxumAdapter::new(self.router.take().unwrap_or_default());
        self.run_inner(adapter, shutdown).await
    }

    /// Run with Ctrl+C and a caller-supplied adapter.
    ///
    /// The adapter owns routing; combining this with
    /// [`Self::routes`]/[`Self::openapi_router`] is rejected.
    pub async fn run_with<A>(self, adapter: A) -> Result<(), ProviderError>
    where
        A: Adapter,
    {
        self.run_with_until(adapter, std::future::pending()).await
    }

    /// Run with Ctrl+C, a caller-supplied adapter, and an extra shutdown future.
    pub async fn run_with_until<A, F>(self, adapter: A, shutdown: F) -> Result<(), ProviderError>
    where
        A: Adapter,
        F: Future<Output = ()> + Send,
    {
        #[cfg(feature = "axum")]
        if self.router.is_some() {
            return Err(ProviderError::InvalidConfig(
                "routes(...)/openapi_router(...) belong to the default axum adapter; \
                 use run()/run_until() or mount those routes in the custom adapter"
                    .to_owned(),
            ));
        }
        self.run_inner(adapter, shutdown).await
    }

    async fn run_inner<A, F>(self, adapter: A, shutdown: F) -> Result<(), ProviderError>
    where
        A: Adapter,
        F: Future<Output = ()> + Send,
    {
        self.validate()?;
        let client = self.gateway_client.clone().unwrap_or_default();
        let api_info = self.resolve_api_info()?;

        let listener = TcpListener::bind(("127.0.0.1", self.port)).await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let health_check = self.health_check.clone().unwrap_or_else(|| {
            Arc::new(DefaultHealthCheck::new(
                self.service.clone(),
                self.version.clone(),
            ))
        });
        let endpoints = ControlEndpoints::new(
            self.service.clone(),
            self.version.clone(),
            self.probe_path.clone(),
            self.notify_path.clone(),
            client.clone(),
            health_check,
        );
        let context = AdapterContext {
            listener,
            endpoints,
            shutdown: ShutdownSignal::new(shutdown_rx),
        };
        let (server, mut monitor) = spawn_adapter(adapter, context);

        let external_shutdown = async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = shutdown => {}
            }
        };
        tokio::pin!(external_shutdown);

        let mut options = RegisterOptions::new(
            address.port(),
            self.version.clone(),
            self.probe_path.clone(),
        )
        .api_info(api_info)
        .notify_url(format!(
            "http://127.0.0.1:{}{}",
            address.port(),
            self.notify_path
        ));
        if let Some(project_root) = &self.project_root {
            options = options.project_root(project_root.clone());
        }
        if let Some(probe_timeout_ms) = self.probe_timeout_ms {
            options = options.probe_timeout_ms(probe_timeout_ms);
        }

        // The listener is already bound; give the adapter a chance to start
        // accepting before the gateway probes.
        tokio::task::yield_now().await;

        let lifecycle = async {
            let registration = register_with_retry(
                &client,
                &self.service,
                options,
                &self.register_retry,
                &mut external_shutdown,
                &mut monitor,
            )
            .await?;
            if registration.is_some() {
                tokio::select! {
                    _ = &mut external_shutdown => {}
                    result = monitor.wait() => {
                        return Err(match result {
                            Ok(()) => ProviderError::Server(
                                "provider server stopped before shutdown was requested".to_owned(),
                            ),
                            Err(error) => error,
                        });
                    }
                }
                if self.unregister_on_shutdown {
                    let _ =
                        tokio::time::timeout(UNREGISTER_TIMEOUT, client.unregister(&self.service))
                            .await;
                }
            }
            Ok(())
        }
        .await;

        let _ = shutdown_tx.send(true);
        match lifecycle {
            Ok(()) => monitor.wait().await,
            Err(error) => {
                let _ = tokio::time::timeout(UNREGISTER_TIMEOUT, monitor.wait()).await;
                server.abort();
                Err(error)
            }
        }
    }

    fn validate(&self) -> Result<(), ProviderError> {
        if self.service.trim().is_empty() {
            return Err(ProviderError::InvalidConfig(
                "service name must not be empty".to_owned(),
            ));
        }
        if self.version.trim().is_empty() {
            return Err(ProviderError::InvalidConfig(
                "version must not be empty".to_owned(),
            ));
        }
        validate_path("probe_path", &self.probe_path)?;
        validate_path("notify_path", &self.notify_path)?;
        if self.probe_path == self.notify_path {
            return Err(ProviderError::InvalidConfig(
                "probe_path and notify_path must differ".to_owned(),
            ));
        }
        if let Some(timeout_ms) = self.probe_timeout_ms
            && !(MIN_PROBE_TIMEOUT_MS..=MAX_PROBE_TIMEOUT_MS).contains(&timeout_ms)
        {
            return Err(ProviderError::InvalidConfig(format!(
                "probe_timeout_ms must be between {MIN_PROBE_TIMEOUT_MS} and {MAX_PROBE_TIMEOUT_MS}"
            )));
        }
        #[cfg(feature = "axum")]
        for path in &self.business_paths {
            if path == &self.probe_path || path == &self.notify_path {
                return Err(ProviderError::RouteConflict(format!(
                    "built-in path {path:?} is already registered by the business router; \
                     change probe_path/notify_path or remove the business route"
                )));
            }
        }
        Ok(())
    }

    fn resolve_api_info(&self) -> Result<Value, ProviderError> {
        if let Some(api_info) = &self.api_info {
            return openapi::normalize_api_info(
                api_info.clone(),
                &self.service,
                &self.version,
                &self.probe_path,
            );
        }

        #[cfg(feature = "axum")]
        {
            openapi::resolve_typed_openapi(
                self.explicit_openapi.clone(),
                self.collected_openapi.clone(),
                &self.service,
                &self.version,
                &self.probe_path,
            )
        }
        #[cfg(not(feature = "axum"))]
        {
            Ok(openapi::minimal_api_info(
                &self.service,
                &self.version,
                &self.probe_path,
            ))
        }
    }
}

fn validate_path(name: &str, path: &str) -> Result<(), ProviderError> {
    if path.is_empty() {
        return Err(ProviderError::InvalidConfig(format!(
            "{name} must not be empty"
        )));
    }
    if !path.starts_with('/') {
        return Err(ProviderError::InvalidConfig(format!(
            "{name} must start with '/'"
        )));
    }
    if path.chars().any(|c| c.is_whitespace() || c == '#') {
        return Err(ProviderError::InvalidConfig(format!(
            "{name} must not contain whitespace or '#'"
        )));
    }
    Ok(())
}

async fn register_with_retry<F>(
    client: &GatewayClient,
    service: &str,
    options: RegisterOptions,
    retry: &RegisterRetry,
    shutdown: &mut F,
    server: &mut ServerMonitor,
) -> Result<Option<RegistrationResponse>, ProviderError>
where
    F: Future<Output = ()> + Unpin,
{
    let started = Instant::now();
    let mut delay = retry.initial_delay.min(retry.max_delay);
    let mut attempts = 0u32;
    let last_error: SdkError;

    loop {
        attempts += 1;
        match client.register(service, options.clone()).await {
            Ok(response) => return Ok(Some(response)),
            Err(error) if should_retry(&error) => {
                if started.elapsed() + delay > retry.window {
                    last_error = error;
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = &mut *shutdown => return Ok(None),
                    result = server.wait() => {
                        return Err(match result {
                            Ok(()) => ProviderError::Server(
                                "provider server stopped before shutdown was requested".to_owned(),
                            ),
                            Err(error) => error,
                        });
                    }
                }
                delay = (delay * 2).min(retry.max_delay);
            }
            Err(error) => return Err(ProviderError::Register(error)),
        }
    }

    Err(ProviderError::RegisterExhausted {
        attempts,
        last: last_error,
    })
}

fn should_retry(error: &SdkError) -> bool {
    error.is_transport()
        || matches!(
            error,
            SdkError::Api { status, .. } if *status >= 500
        )
}

/// Owns the adapter task and exposes its outcome through a watch channel so
/// several `select!` sites can observe it without polling a `JoinHandle` twice.
struct ServerMonitor {
    done: watch::Receiver<bool>,
    result: Arc<Mutex<Option<Result<(), ProviderError>>>>,
}

impl ServerMonitor {
    async fn wait(&mut self) -> Result<(), ProviderError> {
        loop {
            if *self.done.borrow() {
                break;
            }
            if self.done.changed().await.is_err() {
                break;
            }
        }
        let result = self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        result.unwrap_or_else(|| {
            Err(ProviderError::Server(
                "provider server task ended without a result".to_owned(),
            ))
        })
    }
}

fn spawn_adapter<A>(adapter: A, context: AdapterContext) -> (JoinHandle<()>, ServerMonitor)
where
    A: Adapter,
{
    let (done_tx, done_rx) = watch::channel(false);
    let result = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&result);
    let handle = tokio::spawn(async move {
        let _done = DoneGuard(Some(done_tx));
        let outcome = adapter.serve(context).await;
        *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
    });
    (
        handle,
        ServerMonitor {
            done: done_rx,
            result,
        },
    )
}

/// Notifies [`ServerMonitor::wait`] even if the adapter task panics or is
/// aborted.
struct DoneGuard(Option<watch::Sender<bool>>);

impl Drop for DoneGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(true);
        }
    }
}
