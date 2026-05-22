//! A [`reqwest`] transport for smithy-generated clients.
//!
//! [`ReqwestHttpClient`] implements
//! [`aws_smithy_runtime_api::client::http::HttpClient`] and can be installed into
//! AWS SDK for Rust or other smithy-runtime client configurations that accept a
//! smithy HTTP client.
//!
//! # TLS features
//!
//! This crate disables reqwest's default features and forwards TLS selection to
//! reqwest. No TLS backend is enabled by default; enable one of `rustls`,
//! `rustls-no-provider`, `native-tls`, `native-tls-vendored`, or `default-tls`
//! when HTTPS support is required.
//!
//! ```toml
//! smithy-transport-reqwest = { version = "0.1", features = ["native-tls"] }
//! ```
//!
//! # Using with AWS SDK for Rust
//!
//! ```rust,ignore
//! use aws_config::BehaviorVersion;
//! use smithy_transport_reqwest::ReqwestHttpClient;
//!
//! # async fn example() {
//! let sdk_config = aws_config::defaults(BehaviorVersion::latest())
//!     .http_client(ReqwestHttpClient::new())
//!     .load()
//!     .await;
//!
//! let s3 = aws_sdk_s3::Client::new(&sdk_config);
//! # let _ = s3;
//! # }
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use parking_lot::Mutex;

/// A reqwest-backed smithy HTTP client.
///
/// The client lazily creates and caches reqwest clients for each distinct
/// smithy connector timeout configuration. Reqwest's automatic redirect policy
/// is disabled so smithy callers observe service responses directly.
#[derive(Debug)]
pub struct ReqwestHttpClient {
    connector_cache: Mutex<HashMap<CacheKey, SharedHttpConnector>>,
}

impl ReqwestHttpClient {
    /// Creates a new reqwest-backed smithy HTTP client.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self {
            connector_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl HttpClient for ReqwestHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let key = CacheKey::from(settings);
        self.connector_cache
            .lock()
            .entry(key)
            .or_insert_with(|| SharedHttpConnector::new(ReqwestConnector::new(settings)))
            .clone()
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        Some(ConnectorMetadata::new(
            "reqwest",
            Some(Cow::Borrowed("0.13.x")),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CacheKey {
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
}

impl From<&HttpConnectorSettings> for CacheKey {
    fn from(value: &HttpConnectorSettings) -> Self {
        Self {
            connect_timeout: value.connect_timeout(),
            read_timeout: value.read_timeout(),
        }
    }
}

#[derive(Clone, Debug)]
struct ReqwestConnector {
    client: Result<reqwest::Client, Arc<ClientBuildError>>,
}

impl ReqwestConnector {
    fn new(settings: &HttpConnectorSettings) -> Self {
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if let Some(timeout) = settings.connect_timeout() {
            builder = builder.connect_timeout(timeout);
        }
        if let Some(timeout) = settings.read_timeout() {
            builder = builder.read_timeout(timeout);
        }

        Self {
            client: builder
                .build()
                .map_err(|err| Arc::new(ClientBuildError(err.to_string()))),
        }
    }
}

impl HttpConnector for ReqwestConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let client = match &self.client {
            Ok(client) => client.clone(),
            Err(err) => {
                let err = ClientBuildError(err.0.clone());
                return HttpConnectorFuture::ready(Err(ConnectorError::other(Box::new(err), None)));
            }
        };

        let request = match request.try_into_http1x() {
            Ok(request) => request.map(reqwest::Body::wrap),
            Err(err) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(Box::new(err))));
            }
        };

        let request = match reqwest::Request::try_from(request) {
            Ok(request) => request,
            Err(err) => {
                return HttpConnectorFuture::ready(Err(map_reqwest_error(err)));
            }
        };

        HttpConnectorFuture::new(async move {
            let response: http::Response<reqwest::Body> = client
                .execute(request)
                .await
                .map_err(map_reqwest_error)?
                .into();
            let response = response.map(SdkBody::from_body_1_x);

            HttpResponse::try_from(response)
                .map_err(|err| ConnectorError::other(Box::new(err), None))
        })
    }
}

#[derive(Debug)]
struct ClientBuildError(String);

impl fmt::Display for ClientBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ClientBuildError {}

fn map_reqwest_error(err: reqwest::Error) -> ConnectorError {
    if err.is_timeout() {
        ConnectorError::timeout(Box::new(err))
    } else if err.is_request() || err.is_builder() {
        ConnectorError::user(Box::new(err))
    } else if err.is_connect() {
        ConnectorError::io(Box::new(err)).never_connected()
    } else if err.is_body() || err.is_decode() {
        ConnectorError::io(Box::new(err))
    } else {
        ConnectorError::other(Box::new(err), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder;
    use http_body_util::BodyExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn sends_request_and_streams_response_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0; 1024];

            loop {
                let bytes_read = socket.read(&mut chunk).await.unwrap();
                assert_ne!(0, bytes_read);
                buffer.extend_from_slice(&chunk[..bytes_read]);

                if let Some(header_end) = find_subsequence(&buffer, b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buffer[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or_default();

                    if buffer.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }

            let request = String::from_utf8_lossy(&buffer);

            assert!(request.starts_with("POST /hello?x=1 HTTP/1.1"));
            assert!(request.contains("x-test: ok"));
            assert!(request.contains("\r\n\r\nping"));

            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nx-answer: yes\r\n\r\nworld")
                .await
                .unwrap();
        });

        let client = ReqwestHttpClient::new();
        let settings = HttpConnectorSettings::builder()
            .connect_timeout(Duration::from_secs(1))
            .read_timeout(Duration::from_secs(1))
            .build();
        let runtime_components = RuntimeComponentsBuilder::for_tests().build().unwrap();
        let connector = client.http_connector(&settings, &runtime_components);

        let request = http::Request::builder()
            .method("POST")
            .uri(format!("http://{address}/hello?x=1"))
            .header("x-test", "ok")
            .body(SdkBody::from("ping"))
            .unwrap();
        let request = HttpRequest::try_from(request).unwrap();

        let response = connector.call(request).await.unwrap();
        assert_eq!(200, response.status().as_u16());
        assert_eq!("yes", response.headers().get("x-answer").unwrap());

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!("world", body);

        server.await.unwrap();
    }

    #[test]
    fn connector_metadata_identifies_reqwest() {
        let metadata = ReqwestHttpClient::new().connector_metadata().unwrap();
        assert_eq!("reqwest", metadata.name());
        assert_eq!(Some(Cow::Borrowed("0.13.x")), metadata.version());
    }

    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }
}
