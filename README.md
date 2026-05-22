# smithy-transport-reqwest

A small [`reqwest`](https://crates.io/crates/reqwest)-backed implementation of
`aws_smithy_runtime_api::client::http::HttpClient`.

```rust
use smithy_transport_reqwest::ReqwestHttpClient;

let http_client = ReqwestHttpClient::new();
```

The client caches reqwest connectors by smithy HTTP connector settings and
applies smithy connect and read timeout values when creating the underlying
reqwest clients. Reqwest's automatic redirect policy is disabled so generated
smithy clients receive service responses directly.

## TLS backend selection

Reqwest default features are disabled by this crate, and no TLS backend is
enabled by default. Select the backend you want:

```toml
smithy-transport-reqwest = { version = "0.1", features = ["rustls"] }
```

Or, for native TLS:

```toml
smithy-transport-reqwest = { version = "0.1", features = ["native-tls"] }
```

Forwarded TLS features:

- `rustls`
- `rustls-no-provider`
- `default-tls`
- `native-tls`
- `native-tls-no-alpn`
- `native-tls-vendored`
- `native-tls-vendored-no-alpn`

Additional reqwest feature passthroughs are available for `http2`, `http3`, and
`system-proxy`.

## AWS SDK setup

Install this crate with a TLS feature, plus `aws-config` and whichever generated
AWS SDK crates your application uses:

```toml
[dependencies]
aws-config = "1"
aws-sdk-s3 = "1"
smithy-transport-reqwest = { version = "0.1", features = ["rustls"] }
```

Then pass the HTTP client into the shared AWS config loader:

```rust
use aws_config::BehaviorVersion;
use smithy_transport_reqwest::ReqwestHttpClient;

# async fn example() {
let sdk_config = aws_config::defaults(BehaviorVersion::latest())
    .http_client(ReqwestHttpClient::new())
    .load()
    .await;

let s3 = aws_sdk_s3::Client::new(&sdk_config);
# let _ = s3;
# }
```
