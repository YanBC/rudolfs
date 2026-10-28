//! A local Swift-style OSS service for backend integration tests.
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use hyper::{
    Body, Method, Request, Response,
    service::{make_service_fn, service_fn},
};

type Objects = Arc<Mutex<HashMap<String, Bytes>>>;

pub struct Mock {
    pub endpoint: String,
    objects: Objects,
    task: tokio::task::JoinHandle<()>,
}

impl Mock {
    pub fn start() -> Self {
        let objects = Objects::default();
        let shared = objects.clone();
        let service = make_service_fn(move |_| {
            let objects = shared.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    handle(req, objects.clone())
                }))
            }
        });
        let server =
            hyper::Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(service);
        let endpoint = format!("http://{}", server.local_addr());
        let task = tokio::spawn(async move { server.await.unwrap() });
        Self {
            endpoint,
            objects,
            task,
        }
    }

    pub fn objects(&self) -> HashMap<String, Bytes> {
        self.objects.lock().unwrap().clone()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(
    req: Request<Body>,
    objects: Objects,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_owned();
    let mut response = Response::builder();
    let body = if path == "/auth/v1.0" {
        assert_eq!(req.method(), Method::GET);
        assert_eq!(req.headers()["X-Auth-User"], "user");
        assert_eq!(req.headers()["X-Auth-Key"], "secret");
        response = response.header("X-Auth-Token", "token");
        Body::empty()
    } else {
        assert_eq!(req.headers()["X-Auth-Token"], "token");
        assert!(!req.headers().contains_key("X-Auth-Key"));
        assert!(!req.headers().contains_key("X-Delete-After"));
        if path == "/v1/AUTH_TEST/bucket" {
            if req.method() == Method::HEAD {
                Body::empty()
            } else {
                assert_eq!(req.method(), Method::GET);
                let url = reqwest::Url::parse(&format!(
                    "http://localhost{}",
                    req.uri()
                ))
                .unwrap();
                let params: std::collections::HashMap<_, _> =
                    url.query_pairs().collect();
                assert_eq!(params["format"], "json");
                assert_eq!(params["prefix"], "lfs/");
                let objects = objects.lock().unwrap();
                let mut entries: Vec<_> = objects
                    .iter()
                    .map(|(path, data)| {
                        (
                            path.strip_prefix("/v1/AUTH_TEST/bucket/").unwrap(),
                            data.len(),
                        )
                    })
                    .filter(|(name, _)| *name > params["marker"].as_ref())
                    .collect();
                entries.sort();
                let entries: Vec<_> = entries.into_iter().take(1).map(|(name, bytes)| serde_json::json!({"name":name,"bytes":bytes})).collect();
                Body::from(serde_json::to_vec(&entries).unwrap())
            }
        } else {
            assert!(path.starts_with("/v1/AUTH_TEST/bucket/lfs/org/repo/"));
            if req.method() == Method::PUT {
                assert_eq!(
                    req.headers()["Content-Type"],
                    "application/octet-stream"
                );
                let len: usize = req.headers()["Content-Length"]
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                let bytes =
                    hyper::body::to_bytes(req.into_body()).await.unwrap();
                assert_eq!(bytes.len(), len);
                objects.lock().unwrap().insert(path, bytes);
                response = response.status(201);
                Body::empty()
            } else {
                assert!(matches!(*req.method(), Method::GET | Method::HEAD));
                let objects = objects.lock().unwrap();
                match objects.get(&path) {
                    Some(bytes) => {
                        response =
                            response.header("Content-Length", bytes.len());
                        if req.method() == Method::HEAD {
                            Body::empty()
                        } else {
                            Body::from(bytes.clone())
                        }
                    }
                    None => {
                        response = response.status(404);
                        Body::empty()
                    }
                }
            }
        }
    };
    Ok(response.body(body).unwrap())
}
