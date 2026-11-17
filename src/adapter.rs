//! Adapter seam between the lifecycle and a concrete HTTP framework.

use std::future::Future;

use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::endpoints::ControlEndpoints;
use crate::error::ProviderError;

/// Everything an adapter needs to serve a provider.
pub struct AdapterContext {
    /// Listener already bound by the library on `127.0.0.1`.
    pub listener: TcpListener,
    /// Built-in probe and notification handlers.
    pub endpoints: ControlEndpoints,
    /// Fires after optional unregistration; the adapter should stop serving
    /// gracefully and return.
    pub shutdown: ShutdownSignal,
}

/// One-shot shutdown notification shared by the library and its adapter.
#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    receiver: watch::Receiver<bool>,
}

impl ShutdownSignal {
    pub(crate) fn new(receiver: watch::Receiver<bool>) -> Self {
        Self { receiver }
    }

    /// Whether shutdown has already been requested.
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Resolve once shutdown is requested.
    ///
    /// This future is cancel-safe and can be polled by adapters inside
    /// `tokio::select!`.
    pub async fn cancelled(&mut self) {
        if self.is_shutdown() {
            return;
        }
        while self.receiver.changed().await.is_ok() {
            if self.is_shutdown() {
                return;
            }
        }
    }
}

/// HTTP framework integration point.
///
/// Implementations serve the already-bound `listener`, mount the built-in
/// endpoints from [`AdapterContext::endpoints`] alongside their own routes, and
/// return once [`AdapterContext::shutdown`] fires (graceful stop).
pub trait Adapter: Send + 'static {
    /// Serve the provider until the shutdown signal fires.
    fn serve(
        self,
        context: AdapterContext,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send + 'static;
}
