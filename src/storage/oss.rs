//! SF Express OSS (Swift auth v1), matching the sf-oss-golang wire protocol.
use std::{io, time::Duration};

use anyhow::{Context, Error, bail, ensure};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};
use reqwest::{Client, Method, Response, StatusCode, Url, header};

use super::{LFSObject, Storage, StorageKey, StorageStream};

#[derive(Clone)]
pub struct Backend {
    client: Client,
    auth_url: Url,
    container_url: Url,
    auth_user: header::HeaderValue,
    auth_key: header::HeaderValue,
    prefix: String,
}

impl Backend {
    pub async fn new(
        endpoint: String,
        namescope: String,
        container: String,
        auth_user: String,
        auth_key: String,
        prefix: String,
        proxy: Option<String>,
    ) -> Result<Self, Error> {
        let mut base = Url::parse(&endpoint).context("Invalid OSS endpoint")?;
        ensure!(
            matches!(base.scheme(), "http" | "https")
                && base.host_str().is_some()
                && base.username().is_empty()
                && base.password().is_none()
                && base.query().is_none()
                && base.fragment().is_none()
                && matches!(base.path(), "" | "/"),
            "OSS endpoint must be an HTTP(S) origin without credentials, path, query or fragment"
        );
        for (name, value) in
            [("namescope", &namescope), ("container", &container)]
        {
            ensure!(
                !value.is_empty()
                    && value != "."
                    && value != ".."
                    && !value.contains('/'),
                "Invalid OSS {name}"
            );
        }
        ensure!(
            !auth_user.is_empty() && !auth_key.is_empty(),
            "OSS credentials must not be empty"
        );
        let auth_url = base.join("auth/v1.0")?;
        base.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid OSS endpoint"))?
            .clear()
            .push("v1")
            .push(&format!("AUTH_{namescope}"))
            .push(&container);
        let prefix = prefix.trim_matches('/').to_owned();
        ensure!(
            !prefix.split('/').any(|s| s == "." || s == ".."),
            "OSS prefix cannot contain . or .. segments"
        );
        let mut client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(600));
        if let Some(proxy) = proxy {
            client = client.proxy(
                reqwest::Proxy::all(proxy).context("Invalid OSS proxy")?,
            );
        }
        let mut auth_user: header::HeaderValue =
            auth_user.parse().context("Invalid OSS auth user")?;
        let mut auth_key: header::HeaderValue =
            auth_key.parse().context("Invalid OSS auth key")?;
        auth_user.set_sensitive(true);
        auth_key.set_sensitive(true);
        let backend = Self {
            client: client.build()?,
            auth_url,
            container_url: base,
            auth_user,
            auth_key,
            prefix,
        };
        // Catch credentials and container configuration errors at startup.
        let response = backend
            .request(Method::HEAD, backend.container_url.clone())
            .await?
            .send()
            .await?;
        Self::check(response, "HEAD container")?;
        Ok(backend)
    }

    fn object_url(&self, key: &StorageKey) -> Url {
        let mut url = self.container_url.clone();
        let path = format!("{}/{}", key.namespace(), key.oid().path());
        let mut segments = url.path_segments_mut().expect("validated HTTP URL");
        if !self.prefix.is_empty() {
            segments.extend(self.prefix.split('/'));
        }
        segments.extend(path.split('/'));
        drop(segments);
        url
    }

    async fn request(
        &self,
        method: Method,
        url: Url,
    ) -> Result<reqwest::RequestBuilder, Error> {
        // Like the Go SDK, obtain a fresh token for each operation. This also
        // avoids caching expired tokens or replaying a consumed upload stream.
        let response = self
            .client
            .get(self.auth_url.clone())
            .header("X-Auth-User", self.auth_user.clone())
            .header("X-Auth-Key", self.auth_key.clone())
            .send()
            .await?;
        let response = Self::check(response, "authenticate")?;
        let mut token = response
            .headers()
            .get("X-Auth-Token")
            .filter(|t| !t.is_empty())
            .context("OSS auth response missing X-Auth-Token")?
            .clone();
        token.set_sensitive(true);
        Ok(self
            .client
            .request(method, url)
            .header("X-Auth-Token", token))
    }

    fn check(response: Response, operation: &str) -> Result<Response, Error> {
        if !response.status().is_success() {
            // Do not include service bodies, which may contain credentials.
            bail!("OSS {operation} failed with HTTP {}", response.status());
        }
        Ok(response)
    }

    fn content_length(response: &Response) -> Result<u64, Error> {
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .context("OSS response missing Content-Length")?
            .to_str()?
            .parse()
            .context("Invalid OSS Content-Length")
    }
}

#[async_trait]
impl Storage for Backend {
    type Error = Error;

    async fn get(&self, key: &StorageKey) -> Result<Option<LFSObject>, Error> {
        let response = self
            .request(Method::GET, self.object_url(key))
            .await?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = Self::check(response, "GET object")?;
        let len = Self::content_length(&response)?;
        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some(LFSObject::new(len, Box::pin(stream))))
    }

    async fn put(
        &self,
        key: StorageKey,
        value: LFSObject,
    ) -> Result<(), Error> {
        let (len, stream) = value.into_parts();
        let response = self
            .request(Method::PUT, self.object_url(&key))
            .await?
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, len)
            // Deliberately omit the SDK's X-Delete-After: LFS is permanent.
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await?;
        Self::check(response, "PUT object")?;
        Ok(())
    }

    async fn size(&self, key: &StorageKey) -> Result<Option<u64>, Error> {
        let response = self
            .request(Method::HEAD, self.object_url(key))
            .await?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = Self::check(response, "HEAD object")?;
        Ok(Some(Self::content_length(&response)?))
    }

    // As with S3, verification must never delete permanent storage objects.
    async fn delete(&self, _key: &StorageKey) -> Result<(), Error> {
        Ok(())
    }

    fn list(&self) -> StorageStream<(StorageKey, u64), Error> {
        Box::pin(stream::empty())
    }

    fn inventory(&self) -> StorageStream<(StorageKey, u64), Error> {
        let backend = self.clone();
        Box::pin(async_stream::try_stream! {
            #[derive(serde::Deserialize)]
            struct Entry { name: String, bytes: u64 }
            let prefix = if backend.prefix.is_empty() { String::new() } else { format!("{}/", backend.prefix) };
            let mut marker = String::new();
            loop {
                let mut url = backend.container_url.clone();
                url.query_pairs_mut().append_pair("format", "json").append_pair("prefix", &prefix)
                    .append_pair("limit", "1000").append_pair("marker", &marker);
                let response = backend.request(Method::GET, url).await?.send().await?;
                if !response.status().is_success() { Err(anyhow::anyhow!("OSS listing failed with HTTP {}", response.status()))?; }
                if response.status() == StatusCode::NO_CONTENT { break; }
                let entries: Vec<Entry> = serde_json::from_slice(&response.bytes().await?)?;
                if entries.is_empty() { break; }
                let next = entries.last().unwrap().name.clone();
                if next <= marker { Err(anyhow::anyhow!("OSS listing did not advance"))?; }
                for entry in entries {
                    if let Some(key) = entry.name.strip_prefix(&prefix).and_then(StorageKey::from_object_path) {
                        yield (key, entry.bytes);
                    }
                }
                marker = next;
            }
        })
    }

    fn public_url(&self, _key: &StorageKey) -> Option<String> {
        None
    }

    async fn upload_url(
        &self,
        _key: &StorageKey,
        _expires_in: Duration,
    ) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt};
    use hyper::{
        Body, Request, Response as HyperResponse,
        service::{make_service_fn, service_fn},
    };
    use std::{
        collections::HashMap,
        convert::Infallible,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Default)]
    struct State {
        objects: Mutex<HashMap<String, Bytes>>,
        auths: AtomicUsize,
        status: AtomicUsize,
        omit_token: bool,
    }

    struct Mock {
        endpoint: String,
        state: Arc<State>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Mock {
        fn start(omit_token: bool) -> Self {
            let state = Arc::new(State {
                omit_token,
                ..State::default()
            });
            let shared = state.clone();
            let service = make_service_fn(move |_| {
                let state = shared.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |req| {
                        handle(req, state.clone())
                    }))
                }
            });
            let server = hyper::Server::bind(&"127.0.0.1:0".parse().unwrap())
                .serve(service);
            let endpoint = format!("http://{}", server.local_addr());
            let task = tokio::spawn(async move { server.await.unwrap() });
            Self {
                endpoint,
                state,
                task,
            }
        }

        async fn backend(&self, prefix: &str) -> Backend {
            Backend::new(
                self.endpoint.clone(),
                "TEST".into(),
                "bucket".into(),
                "user".into(),
                "secret".into(),
                prefix.into(),
                None,
            )
            .await
            .unwrap()
        }
    }

    async fn handle(
        req: Request<Body>,
        state: Arc<State>,
    ) -> Result<HyperResponse<Body>, Infallible> {
        let path = req.uri().path().to_owned();
        let mut response = HyperResponse::builder();
        let body = if path == "/auth/v1.0" {
            assert_eq!(req.method(), hyper::Method::GET);
            assert_eq!(req.headers()["X-Auth-User"], "user");
            assert_eq!(req.headers()["X-Auth-Key"], "secret");
            assert!(!req.headers().contains_key("X-Auth-Token"));
            state.auths.fetch_add(1, Ordering::SeqCst);
            if !state.omit_token {
                response = response.header("X-Auth-Token", "token");
            }
            Body::empty()
        } else {
            assert_eq!(req.headers()["X-Auth-Token"], "token");
            assert!(!req.headers().contains_key("X-Auth-Key"));
            assert!(!req.headers().contains_key("X-Delete-After"));
            assert!(
                !req.headers().contains_key("X-Container-Meta-Temp-URL-Key")
            );
            let status = state.status.load(Ordering::SeqCst);
            if status != 0 {
                response = response.status(status as u16);
                Body::empty()
            } else if path == "/v1/AUTH_TEST/bucket" {
                assert_eq!(req.method(), hyper::Method::HEAD);
                Body::empty()
            } else if req.method() == hyper::Method::PUT {
                assert_eq!(
                    req.headers()["Content-Type"],
                    "application/octet-stream"
                );
                let len: usize = req.headers()["Content-Length"]
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                match hyper::body::to_bytes(req.into_body()).await {
                    Ok(bytes) => {
                        assert_eq!(bytes.len(), len);
                        state.objects.lock().unwrap().insert(path, bytes);
                        response = response.status(201);
                    }
                    Err(_) => {
                        response = response.status(400);
                    }
                }
                Body::empty()
            } else {
                assert!(matches!(
                    *req.method(),
                    hyper::Method::GET | hyper::Method::HEAD
                ));
                let objects = state.objects.lock().unwrap();
                if let Some(bytes) = objects.get(&path) {
                    response = response.header("Content-Length", bytes.len());
                    if req.method() == hyper::Method::HEAD {
                        Body::empty()
                    } else {
                        Body::from(bytes.clone())
                    }
                } else {
                    response = response.status(404);
                    Body::empty()
                }
            }
        };
        Ok(response.body(body).unwrap())
    }

    fn key() -> StorageKey {
        StorageKey::new(
            super::super::Namespace::new("org".into(), "repo".into()),
            [0u8; 32].into(),
        )
    }

    #[tokio::test]
    async fn roundtrip_and_missing_objects() {
        let mock = Mock::start(false);
        let backend = mock.backend("/lfs/sp ace/").await;
        let key = key();
        assert!(backend.get(&key).await.unwrap().is_none());
        assert_eq!(backend.size(&key).await.unwrap(), None);
        let bytes = Bytes::from_static(b"hello oss");
        let input =
            stream::iter(vec![Ok(bytes.slice(..3)), Ok(bytes.slice(3..))]);
        backend
            .put(key.clone(), LFSObject::new(9, Box::pin(input)))
            .await
            .unwrap();
        assert_eq!(backend.size(&key).await.unwrap(), Some(9));
        let object = backend.get(&key).await.unwrap().unwrap();
        assert_eq!(object.len(), 9);
        assert_eq!(
            object
                .stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .concat(),
            bytes
        );
        let path = format!(
            "/v1/AUTH_TEST/bucket/lfs/sp%20ace/org/repo/{}",
            key.oid().path()
        );
        assert!(mock.state.objects.lock().unwrap().contains_key(&path));
        backend.delete(&key).await.unwrap();
        assert_eq!(backend.size(&key).await.unwrap(), Some(9));
        assert!(backend.list().next().await.is_none());
        assert!(backend.public_url(&key).is_none());
        assert!(
            backend
                .upload_url(&key, Duration::from_secs(60))
                .await
                .is_none()
        );
        assert_eq!(mock.state.auths.load(Ordering::SeqCst), 7);
    }

    #[tokio::test]
    async fn empty_objects_and_stream_errors() {
        let mock = Mock::start(false);
        let backend = mock.backend("").await;
        backend
            .put(key(), LFSObject::new(0, Box::pin(stream::empty())))
            .await
            .unwrap();
        assert_eq!(backend.size(&key()).await.unwrap(), Some(0));
        let path =
            format!("/v1/AUTH_TEST/bucket/org/repo/{}", key().oid().path());
        assert!(mock.state.objects.lock().unwrap().contains_key(&path));
        let broken =
            stream::once(async { Err(io::Error::other("broken input")) });
        assert!(
            backend
                .put(key(), LFSObject::new(10, Box::pin(broken)))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn errors_are_not_missing_objects() {
        let mock = Mock::start(false);
        let backend = mock.backend("lfs").await;
        for status in [401, 403, 500] {
            mock.state.status.store(status, Ordering::SeqCst);
            assert!(backend.get(&key()).await.is_err());
            assert!(backend.size(&key()).await.is_err());
            assert!(
                backend
                    .put(key(), LFSObject::new(0, Box::pin(stream::empty())))
                    .await
                    .is_err()
            );
        }
        assert!(
            Backend::new(
                mock.endpoint.clone(),
                "TEST".into(),
                "bucket".into(),
                "user".into(),
                "secret".into(),
                "lfs".into(),
                None
            )
            .await
            .is_err()
        );
        let mock = Mock::start(true);
        let error = Backend::new(
            mock.endpoint.clone(),
            "TEST".into(),
            "bucket".into(),
            "user".into(),
            "secret".into(),
            "lfs".into(),
            None,
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("missing X-Auth-Token"));
    }

    #[tokio::test]
    async fn explicit_proxy_carries_auth_and_object_requests() {
        let proxy = Mock::start(false);
        let backend = Backend::new(
            "http://oss.invalid".into(),
            "TEST".into(),
            "bucket".into(),
            "user".into(),
            "secret".into(),
            "lfs".into(),
            Some(proxy.endpoint.clone()),
        )
        .await
        .unwrap();
        assert!(backend.size(&key()).await.unwrap().is_none());
        backend
            .put(key(), LFSObject::new(0, Box::pin(stream::empty())))
            .await
            .unwrap();
        assert_eq!(backend.get(&key()).await.unwrap().unwrap().len(), 0);
        assert_eq!(proxy.state.auths.load(Ordering::SeqCst), 4);
    }
}
