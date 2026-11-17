//! End-to-end provider tests against an in-process mock management gateway.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use localgateway_provider::{
    GatewayClient, HealthCheckFn, HealthResponse, PatchOptions, ProviderBuilder, ProviderError,
    RegisterRetry,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct MockState {
    handshakes: AtomicUsize,
    puts: AtomicUsize,
    patches: AtomicUsize,
    deletes: AtomicUsize,
    valid_token: Mutex<String>,
    put_bodies: Mutex<Vec<Value>>,
    patch_bodies: Mutex<Vec<Value>>,
}

impl MockState {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

fn session_json(token: &str) -> Value {
    json!({
        "token": token,
        "expiresAt": "2026-11-13T20:00:00Z",
        "gateway": {"baseUrl": "http://127.0.0.1:1", "version": "0.1.0", "pid": 10},
        "caller": {"pid": 20, "userSid": "S-1-5-21-1", "imagePath": "C:\\provider.exe"},
        "policy": {
            "probeIntervalMs": 10000,
            "probeTimeoutMs": 2000,
            "offlineAfterMisses": 3,
            "expireAfterMs": 60000
        }
    })
}

fn registration_json(name: &str, revision: i64) -> Value {
    json!({
        "name": name,
        "status": "healthy",
        "lastProbe": "2026-11-13T20:00:00Z",
        "revision": revision
    })
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(json!({"code": "unauthorized", "message": "missing or expired token"})),
    )
        .into_response()
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned)
}

fn is_current_token(state: &MockState, headers: &HeaderMap) -> bool {
    let valid = state.valid_token.lock().expect("token lock").clone();
    bearer_token(headers).as_deref() == Some(valid.as_str())
}

async fn create_session(State(state): State<Arc<MockState>>) -> impl IntoResponse {
    let attempt = state.handshakes.fetch_add(1, Ordering::SeqCst) + 1;
    let token = format!("token-{attempt}");
    *state.valid_token.lock().expect("token lock") = token.clone();
    axum::Json(session_json(&token))
}

async fn register_service(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_current_token(&state, &headers) {
        return unauthorized();
    }
    let attempt = state.puts.fetch_add(1, Ordering::SeqCst) + 1;
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state
        .put_bodies
        .lock()
        .expect("body log")
        .push(parsed.clone());
    let name = parsed
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("provider-it");
    axum::Json(registration_json(name, attempt as i64)).into_response()
}

async fn patch_service(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_current_token(&state, &headers) {
        state.patches.fetch_add(1, Ordering::SeqCst);
        return unauthorized();
    }
    let attempt = state.patches.fetch_add(1, Ordering::SeqCst) + 1;
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.patch_bodies.lock().expect("body log").push(parsed);
    axum::Json(registration_json("provider-it", attempt as i64 + 10)).into_response()
}

async fn unregister_service(State(state): State<Arc<MockState>>, headers: HeaderMap) -> Response {
    if !is_current_token(&state, &headers) {
        return unauthorized();
    }
    state.deletes.fetch_add(1, Ordering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}

fn mock_app(state: Arc<MockState>) -> Router {
    Router::new()
        .route("/gw/session", post(create_session))
        .route(
            "/gw/services/{name}",
            put(register_service)
                .patch(patch_service)
                .delete(unregister_service),
        )
        .with_state(state)
}

struct MockGateway {
    base_url: String,
    state: Arc<MockState>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl MockGateway {
    async fn start(state: Arc<MockState>) -> Self {
        Self::start_on(0, state).await
    }

    async fn start_on(port: u16, state: Arc<MockState>) -> Self {
        let app = mock_app(Arc::clone(&state));
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind mock gateway");
        let address = listener.local_addr().expect("mock address");
        let (shutdown, mut stop) = oneshot::channel();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = (&mut stop).await;
                })
                .await
                .expect("serve mock gateway");
        });
        Self {
            base_url: format!("http://{address}"),
            state,
            shutdown: Some(shutdown),
            handle,
        }
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.await.expect("mock gateway stops");
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("test HTTP client")
}

fn sdk_client(base_url: &str) -> GatewayClient {
    GatewayClient::builder()
        .base_url(base_url)
        .http_client(http_client())
        .build()
}

fn fast_retry() -> RegisterRetry {
    RegisterRetry::new(
        Duration::from_millis(25),
        Duration::from_millis(100),
        Duration::from_secs(5),
    )
}

async fn wait_for_put(state: &MockState) -> Value {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if let Some(body) = state.put_bodies.lock().expect("body log").first().cloned() {
            return body;
        }
        assert!(Instant::now() < deadline, "registration timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind free port probe");
    let port = listener.local_addr().expect("probe address").port();
    drop(listener);
    port
}

fn provider_port(body: &Value) -> u16 {
    body.get("port")
        .and_then(Value::as_u64)
        .expect("registration port") as u16
}

#[tokio::test]
async fn minimal_configuration_registers_serves_health_and_unregisters() {
    let gateway = MockGateway::start(MockState::new()).await;
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("provider-it", "1.2.3")
            .gateway_client(sdk_client(&gateway.base_url))
            .register_retry(fast_retry())
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    let body = wait_for_put(&gateway.state).await;
    let port = provider_port(&body);
    assert!(port > 0, "an ephemeral port is reported");
    assert_eq!(body["version"], "1.2.3");
    assert_eq!(body["protocol"], "http");
    assert_eq!(body["probe"]["path"], "/health");
    assert_eq!(
        body["notifyUrl"],
        format!("http://127.0.0.1:{port}/_localgateway/events")
    );
    assert_eq!(body["apiInfo"]["info"]["title"], "provider-it");
    assert_eq!(body["apiInfo"]["info"]["version"], "1.2.3");
    assert!(body["apiInfo"]["paths"]["/health"]["get"].is_object());

    let response = http_client()
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(response.status(), 200);
    let health: Value = response.json().await.expect("health body");
    assert_eq!(
        health,
        json!({"status": "ok", "service": "provider-it", "version": "1.2.3"})
    );

    let _ = stop_tx.send(());
    provider
        .await
        .expect("provider task")
        .expect("provider exits cleanly");
    assert_eq!(gateway.state.deletes.load(Ordering::SeqCst), 1);
}

#[utoipa::path(get, path = "/api/hello", responses((status = 200, description = "greeting")))]
async fn hello() -> &'static str {
    "hello"
}

#[tokio::test]
async fn openapi_router_collects_business_operations_and_serves_them() {
    let gateway = MockGateway::start(MockState::new()).await;
    let router = OpenApiRouter::new().routes(routes!(hello));
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("provider-openapi", "2.0.0")
            .gateway_client(sdk_client(&gateway.base_url))
            .register_retry(fast_retry())
            .openapi_router(router)
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    let body = wait_for_put(&gateway.state).await;
    let port = provider_port(&body);
    let paths = &body["apiInfo"]["paths"];
    assert!(paths["/api/hello"]["get"].is_object());
    assert!(paths["/health"]["get"].is_object());
    assert_eq!(body["apiInfo"]["info"]["title"], "provider-openapi");
    assert_eq!(body["apiInfo"]["info"]["version"], "2.0.0");

    let greeting = http_client()
        .get(format!("http://127.0.0.1:{port}/api/hello"))
        .send()
        .await
        .expect("business request")
        .text()
        .await
        .expect("business body");
    assert_eq!(greeting, "hello");

    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");
}

#[tokio::test]
async fn raw_api_info_is_preserved_and_completed() {
    let gateway = MockGateway::start(MockState::new()).await;
    let raw = json!({
        "openapi": "3.0.3",
        "info": {"title": "custom-title"},
        "paths": {
            "/custom": {"get": {"responses": {"200": {"description": "ok"}}}}
        }
    });
    let routes = Router::new().route("/custom", get(|| async { "custom" }));
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("provider-raw", "3.0.0")
            .gateway_client(sdk_client(&gateway.base_url))
            .register_retry(fast_retry())
            .api_info(raw)
            .routes(routes)
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    let body = wait_for_put(&gateway.state).await;
    let api = &body["apiInfo"];
    assert_eq!(api["openapi"], "3.0.3");
    assert_eq!(api["info"]["title"], "custom-title");
    assert_eq!(api["info"]["version"], "3.0.0");
    assert!(api["paths"]["/custom"]["get"].is_object());
    assert!(api["paths"]["/health"]["get"].is_object());

    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");
}

#[tokio::test]
async fn restart_notification_refreshes_the_management_base_url() {
    let first = MockGateway::start(MockState::new()).await;
    let second = MockGateway::start(MockState::new()).await;
    let client = sdk_client(&first.base_url);
    let client_probe = client.clone();
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("provider-notify", "1.0.0")
            .gateway_client(client)
            .register_retry(fast_retry())
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    let body = wait_for_put(&first.state).await;
    let port = provider_port(&body);
    assert!(first.state.handshakes.load(Ordering::SeqCst) >= 1);

    let notification = http_client()
        .post(format!("http://127.0.0.1:{port}/_localgateway/events"))
        .json(&json!({
            "type": "gateway_restarting",
            "nextBaseUrl": second.base_url,
        }))
        .send()
        .await
        .expect("notification request");
    assert_eq!(notification.status(), 200);
    assert_eq!(
        client_probe.base_url().expect("base URL"),
        second.base_url,
        "notification updates the shared client"
    );

    client_probe
        .patch(
            "provider-notify",
            PatchOptions::new().probe("/health", None),
        )
        .await
        .expect("PATCH reaches the new gateway");
    assert!(second.state.patches.load(Ordering::SeqCst) >= 1);
    assert_eq!(first.state.patches.load(Ordering::SeqCst), 0);

    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");

    first.stop().await;
    second.stop().await;
}

#[tokio::test]
async fn registers_when_the_gateway_starts_after_the_provider() {
    let port = free_port().await;
    let gateway_url = format!("http://127.0.0.1:{port}");
    let client = sdk_client(&gateway_url);
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("late-gateway", "1.0.0")
            .gateway_client(client)
            .register_retry(RegisterRetry::new(
                Duration::from_millis(25),
                Duration::from_millis(100),
                Duration::from_secs(5),
            ))
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    // Let the provider fail at least a couple of registration attempts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let gateway = MockGateway::start_on(port, MockState::new()).await;

    let body = wait_for_put(&gateway.state).await;
    assert_eq!(body["version"], "1.0.0");
    assert_eq!(body["probe"]["path"], "/health");

    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");
}

#[utoipa::path(get, path = "/health", responses((status = 200, description = "conflict")))]
async fn conflicting_health() -> &'static str {
    "nope"
}

#[tokio::test]
async fn openapi_router_route_conflicts_fail_before_binding() {
    let router = OpenApiRouter::new().routes(routes!(conflicting_health));
    let error = ProviderBuilder::new("conflict", "1.0.0")
        .openapi_router(router)
        .run_until(std::future::pending())
        .await
        .expect_err("the probe path conflicts with a business route");
    assert!(matches!(error, ProviderError::RouteConflict(_)));
}

#[tokio::test]
async fn axum_router_route_conflicts_surface_as_startup_errors() {
    let gateway = MockGateway::start(MockState::new()).await;
    let router = Router::new().route("/health", get(|| async { "conflict" }));

    let error = ProviderBuilder::new("router-conflict", "1.0.0")
        .gateway_client(sdk_client(&gateway.base_url))
        .register_retry(fast_retry())
        .routes(router)
        .run_until(std::future::pending())
        .await
        .expect_err("the merged axum router must be rejected");
    assert!(matches!(error, ProviderError::RouteConflict(_)));
}

#[tokio::test]
async fn unregister_on_shutdown_can_be_disabled() {
    let gateway = MockGateway::start(MockState::new()).await;
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("no-unregister", "1.0.0")
            .gateway_client(sdk_client(&gateway.base_url))
            .register_retry(fast_retry())
            .unregister_on_shutdown(false)
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    let _ = wait_for_put(&gateway.state).await;
    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");
    assert_eq!(gateway.state.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_health_check_drives_the_probe_response() {
    let gateway = MockGateway::start(MockState::new()).await;
    let check = Arc::new(HealthCheckFn::new(|| async {
        HealthResponse::new(
            503,
            json!({"status": "degraded", "backend": "unavailable", "retryAfter": 5}),
        )
    }));
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("provider-custom-health", "1.0.0")
            .gateway_client(sdk_client(&gateway.base_url))
            .register_retry(fast_retry())
            .health_check(check)
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    // A failing health check does not prevent registration: the gateway only
    // learns about it through failed probes.
    let body = wait_for_put(&gateway.state).await;
    let port = provider_port(&body);
    assert_eq!(gateway.state.puts.load(Ordering::SeqCst), 1);
    assert_eq!(body["version"], "1.0.0");

    let response = http_client()
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(response.status(), 503);
    let health: Value = response.json().await.expect("health body");
    assert_eq!(
        health,
        json!({"status": "degraded", "backend": "unavailable", "retryAfter": 5})
    );

    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");
}

#[tokio::test]
async fn custom_health_check_can_await_before_responding() {
    let gateway = MockGateway::start(MockState::new()).await;
    let delay = Duration::from_millis(100);
    let check = Arc::new(HealthCheckFn::new(move || async move {
        tokio::time::sleep(delay).await;
        HealthResponse::healthy("async-provider", "9.9.9")
    }));
    let (stop_tx, stop_rx) = oneshot::channel();

    let provider = tokio::spawn(
        ProviderBuilder::new("provider-async-health", "1.0.0")
            .gateway_client(sdk_client(&gateway.base_url))
            .register_retry(fast_retry())
            .health_check(check)
            .run_until(async move {
                let _ = stop_rx.await;
            }),
    );

    let body = wait_for_put(&gateway.state).await;
    let port = provider_port(&body);

    let started = Instant::now();
    let response = http_client()
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .expect("health request");
    assert!(
        started.elapsed() >= delay,
        "the probe awaits the async check"
    );
    assert_eq!(response.status(), 200);
    let health: Value = response.json().await.expect("health body");
    assert_eq!(
        health,
        json!({"status": "ok", "service": "async-provider", "version": "9.9.9"})
    );

    let _ = stop_tx.send(());
    provider.await.expect("provider task").expect("clean exit");
}
