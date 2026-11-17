//! Errors returned while building or running a provider.

use localgateway_sdk::SdkError;

/// Failure modes of [`crate::ProviderBuilder`].
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The builder configuration is locally inconsistent or invalid.
    #[error("invalid provider configuration: {0}")]
    InvalidConfig(String),
    /// A built-in endpoint path is already served by the business router.
    #[error("route conflict: {0}")]
    RouteConflict(String),
    /// The provider could not bind its loopback listener.
    #[error("failed to bind the provider listener: {0}")]
    Bind(#[from] std::io::Error),
    /// Registration failed in a way that is not retried.
    #[error("failed to register the provider: {0}")]
    Register(#[from] SdkError),
    /// Registration kept failing until the retry window ran out.
    #[error("provider registration failed after {attempts} attempt(s): {last}")]
    RegisterExhausted {
        /// Number of `PUT /gw/services/{name}` attempts.
        attempts: u32,
        /// Last error returned by the SDK.
        last: SdkError,
    },
    /// The adapter serving the provider stopped with an error.
    #[error("provider server failed: {0}")]
    Server(String),
}
