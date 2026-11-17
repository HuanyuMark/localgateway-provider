# localgateway-provider

Provider lifecycle glue for LocalGateway (Rust). Supply business routes plus a
few values; the crate binds a loopback listener, serves the probe and restart
notification endpoints, registers with the gateway, retries with backoff and
shuts down gracefully.

## Repository layout

This repository root is the crate root: `Cargo.toml`, `src/` and `tests/` live
at the top level, with no nested workspace or crate directory. Current version:
`0.2.2`.

## Dependencies

`localgateway-provider` depends on `localgateway-sdk` v0.2.2 through GitHub git
+ tag:

```toml
localgateway-sdk = { git = "https://github.com/HuanyuMark/localgateway-sdk", tag = "v0.2.2" }
```

## Usage

```toml
[dependencies]
localgateway-provider = { git = "https://github.com/HuanyuMark/localgateway-provider", tag = "v0.2.2" }
```

```rust
use axum::Router;
use axum::routing::get;
use localgateway_provider::ProviderBuilder;

let routes = Router::new().route("/api/hello", get(|| async { "hello" }));

ProviderBuilder::new("my-service", "0.1.0")
    .port(0)
    .routes(routes)
    .run()
    .await?;
```

`localgateway-provider` brings `localgateway-sdk` and `localgateway-api` with it.
Disable default features to opt out of the `axum` adapter and use
`run_with(adapter)` instead.
