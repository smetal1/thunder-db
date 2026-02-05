//! TLS support for ThunderDB wire protocols.
//!
//! Provides TLS configuration and utilities for secure connections:
//! - Certificate and key loading from PEM files
//! - TLS acceptor creation for server-side TLS
//! - Integration with tokio-rustls for async TLS

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// TLS configuration for protocol servers.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the server certificate file (PEM format)
    pub cert_path: String,
    /// Path to the server private key file (PEM format)
    pub key_path: String,
    /// Optional path to CA certificate for client verification
    pub ca_path: Option<String>,
    /// Whether to require client certificates
    pub require_client_cert: bool,
}

impl TlsConfig {
    /// Create a new TLS configuration.
    pub fn new(cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            ca_path: None,
            require_client_cert: false,
        }
    }

    /// Set the CA certificate path for client verification.
    pub fn with_ca(mut self, ca_path: impl Into<String>) -> Self {
        self.ca_path = Some(ca_path.into());
        self
    }

    /// Require client certificates.
    pub fn require_client_cert(mut self) -> Self {
        self.require_client_cert = true;
        self
    }

    /// Load certificates from PEM file.
    fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
        let file = File::open(path)
            .map_err(|e| TlsError::CertificateLoad(format!("Failed to open {}: {}", path.display(), e)))?;
        let mut reader = BufReader::new(file);

        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::CertificateLoad(format!("Failed to parse certificates: {}", e)))?;

        if certs.is_empty() {
            return Err(TlsError::CertificateLoad(format!(
                "No certificates found in {}",
                path.display()
            )));
        }

        Ok(certs)
    }

    /// Load private key from PEM file.
    fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
        let file = File::open(path)
            .map_err(|e| TlsError::KeyLoad(format!("Failed to open {}: {}", path.display(), e)))?;
        let mut reader = BufReader::new(file);

        // Try to read different key formats
        loop {
            match rustls_pemfile::read_one(&mut reader) {
                Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => {
                    return Ok(PrivateKeyDer::Pkcs1(key));
                }
                Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => {
                    return Ok(PrivateKeyDer::Pkcs8(key));
                }
                Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => {
                    return Ok(PrivateKeyDer::Sec1(key));
                }
                Ok(Some(_)) => {
                    // Skip other PEM items (certificates, etc.)
                    continue;
                }
                Ok(None) => {
                    return Err(TlsError::KeyLoad(format!(
                        "No private key found in {}",
                        path.display()
                    )));
                }
                Err(e) => {
                    return Err(TlsError::KeyLoad(format!("Failed to parse key: {}", e)));
                }
            }
        }
    }

    /// Build a TLS acceptor from this configuration.
    pub fn build_acceptor(&self) -> Result<TlsAcceptor, TlsError> {
        let certs = Self::load_certs(Path::new(&self.cert_path))?;
        let key = Self::load_key(Path::new(&self.key_path))?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Configuration(format!("Failed to build TLS config: {}", e)))?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

/// TLS-related errors.
#[derive(Debug, Clone)]
pub enum TlsError {
    /// Failed to load certificate
    CertificateLoad(String),
    /// Failed to load private key
    KeyLoad(String),
    /// TLS configuration error
    Configuration(String),
    /// TLS handshake error
    Handshake(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::CertificateLoad(msg) => write!(f, "Certificate load error: {}", msg),
            TlsError::KeyLoad(msg) => write!(f, "Key load error: {}", msg),
            TlsError::Configuration(msg) => write!(f, "TLS configuration error: {}", msg),
            TlsError::Handshake(msg) => write!(f, "TLS handshake error: {}", msg),
        }
    }
}

impl std::error::Error for TlsError {}

/// A TLS acceptor that supports hot-reloading certificates from disk.
///
/// Uses [`arc_swap::ArcSwap`] for lock-free reads, so calling [`current()`](Self::current)
/// on every new connection is extremely cheap. Certificate reload via [`reload()`](Self::reload)
/// re-reads the PEM files and atomically swaps the inner acceptor; existing connections
/// are unaffected and continue using the old certificates until they close.
pub struct ReloadableTlsAcceptor {
    inner: arc_swap::ArcSwap<tokio_rustls::TlsAcceptor>,
    config: TlsConfig,
}

impl ReloadableTlsAcceptor {
    /// Create a new reloadable TLS acceptor from the given configuration.
    ///
    /// This reads certificates from disk immediately and returns an error if
    /// they cannot be loaded.
    pub fn new(config: TlsConfig) -> Result<Self, TlsError> {
        let acceptor = config.build_acceptor()?;
        Ok(Self {
            inner: arc_swap::ArcSwap::from_pointee(acceptor),
            config,
        })
    }

    /// Get the current TLS acceptor (cheap, lock-free).
    ///
    /// Call this on each new connection to always use the most recent certificates.
    pub fn current(&self) -> Arc<tokio_rustls::TlsAcceptor> {
        self.inner.load_full()
    }

    /// Reload certificates from disk.
    ///
    /// On success the new acceptor is swapped in atomically; all subsequent calls
    /// to [`current()`](Self::current) will return the updated acceptor. On failure
    /// the previous acceptor remains active and an error is returned.
    pub fn reload(&self) -> Result<(), TlsError> {
        let new_acceptor = self.config.build_acceptor()?;
        self.inner.store(Arc::new(new_acceptor));
        tracing::info!("TLS certificates reloaded successfully");
        Ok(())
    }
}

/// A wrapper around a stream that may or may not be TLS-encrypted.
/// This allows protocol handlers to work with both plain and TLS connections.
pub enum MaybeTlsStream<S> {
    /// Plain unencrypted stream
    Plain(S),
    /// TLS-encrypted stream
    Tls(tokio_rustls::server::TlsStream<S>),
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> tokio::io::AsyncRead for MaybeTlsStream<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for MaybeTlsStream<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_creation() {
        let config = TlsConfig::new("/path/to/cert.pem", "/path/to/key.pem");
        assert_eq!(config.cert_path, "/path/to/cert.pem");
        assert_eq!(config.key_path, "/path/to/key.pem");
        assert!(config.ca_path.is_none());
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_tls_config_with_ca() {
        let config = TlsConfig::new("/path/to/cert.pem", "/path/to/key.pem")
            .with_ca("/path/to/ca.pem")
            .require_client_cert();

        assert_eq!(config.ca_path, Some("/path/to/ca.pem".to_string()));
        assert!(config.require_client_cert);
    }

    #[test]
    fn test_tls_error_display() {
        let err = TlsError::CertificateLoad("test error".to_string());
        assert!(err.to_string().contains("Certificate load error"));

        let err = TlsError::Handshake("handshake failed".to_string());
        assert!(err.to_string().contains("TLS handshake error"));
    }
}
