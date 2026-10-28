//! Integration coverage for the OSS backend through the public LFS API.
//! A local Swift-style mock is used; no private OSS credentials are required.
// The faulty feature deliberately injects random stream failures.
#![cfg(not(feature = "faulty"))]

mod common;

use common::oss::Mock;
use reqwest::Client;
use rudolfs::{Cache, OssServerBuilder};
use serde_json::{Value, json};
use sha2::Digest;

#[tokio::test]
async fn oss_smoke_test_unencrypted() {
    exercise_server(false, false).await;
}

#[tokio::test]
async fn oss_smoke_test_encrypted() {
    exercise_server(true, false).await;
}

#[tokio::test]
async fn oss_smoke_test_cached() {
    exercise_server(false, true).await;
}

#[tokio::test]
async fn oss_smoke_test_encrypted_and_cached() {
    exercise_server(true, true).await;
}

async fn exercise_server(encrypted: bool, cached: bool) {
    let mock = Mock::start();
    let cache = tempfile::TempDir::new().unwrap();
    let mut builder = OssServerBuilder::new(
        mock.endpoint.clone(),
        "TEST".into(),
        "bucket".into(),
        "user".into(),
        "secret".into(),
    );
    if encrypted {
        builder.key([42; 32]);
    }
    if cached {
        builder.cache(Cache::new(cache.path().into(), 1024 * 1024));
    }
    let server = builder.spawn("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let endpoint = format!("http://{}", server.addr());
    let task = tokio::spawn(server);
    let client = Client::builder().no_proxy().build().unwrap();
    let data = b"an LFS object stored in SF OSS";
    let oid = hex::encode(sha2::Sha256::digest(data));
    let object = json!({"oid": oid, "size": data.len()});
    let batch = |operation: &str| json!({"operation": operation, "objects": [object.clone()]});
    let url = format!("{endpoint}/api/org/repo/objects/batch");
    let response = client
        .post(&url)
        .body(batch("upload").to_string())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let result: Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    let upload = result["objects"][0]["actions"]["upload"]["href"]
        .as_str()
        .unwrap();
    assert!(upload.starts_with(&endpoint));
    let response = client.put(upload).body(data.to_vec()).send().await.unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let verify = result["objects"][0]["actions"]["verify"]["href"]
        .as_str()
        .unwrap();
    assert!(
        client
            .post(verify)
            .body(object.to_string())
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let response = client
        .post(&url)
        .body(batch("upload").to_string())
        .send()
        .await
        .unwrap();
    let result: Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert!(result["objects"][0].get("actions").is_none());
    let response = client
        .post(&url)
        .body(batch("download").to_string())
        .send()
        .await
        .unwrap();
    let result: Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    let download = result["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert!(download.starts_with(&endpoint));
    let response = client.get(download).send().await.unwrap();
    assert!(response.status().is_success());
    assert_eq!(response.bytes().await.unwrap().as_ref(), data);
    let inventory: Value = serde_json::from_slice(
        &client
            .get(format!("{endpoint}/api/v1/repositories"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(inventory["pagination"]["total"], 1);
    assert_eq!(inventory["repositories"][0]["organization"], "org");
    assert_eq!(inventory["repositories"][0]["object_count"], 1);
    assert_eq!(inventory["repositories"][0]["size_bytes"], data.len());
    let objects = mock.objects();
    assert_eq!(objects.len(), 1);
    let stored = objects.values().next().unwrap();
    if encrypted {
        assert_ne!(stored.as_ref(), data);
    } else {
        assert_eq!(stored.as_ref(), data);
    }
    drop(objects);
    task.abort();
}
