//! SQLite expectations survive external deletions, corruption and restarts.
use std::path::{Path, PathBuf};

use reqwest::Client;
use rudolfs::LocalServerBuilder;
use serde_json::Value;
use sha2::Digest;

fn oid(data: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(data))
}

fn object_path(root: &Path, data: &[u8]) -> PathBuf {
    let oid = oid(data);
    root.join(format!(
        "objects/org/repo/{}/{}/{}",
        &oid[..2],
        &oid[2..4],
        oid
    ))
}

async fn start(
    root: &Path,
    encrypted: bool,
) -> (String, tokio::task::JoinHandle<hyper::Result<()>>) {
    let mut builder = LocalServerBuilder::new(root.into());
    if encrypted {
        builder.key([42; 32]);
    }
    let server = builder.spawn("127.0.0.1:0".parse().unwrap()).await.unwrap();
    (format!("http://{}", server.addr()), tokio::spawn(server))
}

async fn response_json(response: reqwest::Response) -> Value {
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}

async fn audit(client: &Client, base: &str, query: &str) -> Value {
    let response = client
        .get(format!("{base}/api/v1/storage/audit{query}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    response_json(response).await
}

async fn upload(client: &Client, base: &str, data: &[u8]) {
    let response = client
        .put(format!("{base}/api/org/repo/object/{}", oid(data)))
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_deleted_and_resized_objects_after_restart_and_reupload_repairs_them()
 {
    let client = Client::builder().no_proxy().build().unwrap();
    for encrypted in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (base, task) = start(root.path(), encrypted).await;
        assert_eq!(audit(&client, &base, "").await["tracked_objects"], 0);
        for data in [b"first".as_slice(), b"second", b"third"] {
            upload(&client, &base, data).await;
        }
        // Repeated uploads have only one inventory record per object.
        upload(&client, &base, b"first").await;
        let healthy = audit(&client, &base, "").await;
        assert_eq!(healthy["tracked_objects"], 3);
        assert_eq!(healthy["pagination"]["total"], 0);
        task.abort();
        let _ = task.await;

        std::fs::remove_file(object_path(root.path(), b"first")).unwrap();
        std::fs::write(object_path(root.path(), b"second"), b"!").unwrap();
        let (base, task) = start(root.path(), encrypted).await;
        let page = audit(&client, &base, "?limit=1&q=ORG%2FREPO").await;
        assert_eq!(page["tracked_objects"], 3);
        assert_eq!(page["pagination"]["total"], 2);
        assert_eq!(page["issues"].as_array().unwrap().len(), 1);
        let next = page["pagination"]["next"].as_str().unwrap();
        let next_page: Value = response_json(
            client.get(format!("{base}{next}")).send().await.unwrap(),
        )
        .await;
        assert!(next_page["pagination"]["next"].is_null());
        assert_ne!(page["issues"][0]["oid"], next_page["issues"][0]["oid"]);
        let all = audit(&client, &base, "").await;
        let issues = all["issues"].as_array().unwrap();
        let missing = issues.iter().find(|v| v["status"] == "missing").unwrap();
        assert_eq!(missing["oid"], oid(b"first"));
        assert_eq!(missing["expected_size_bytes"], 5);
        assert!(missing["actual_size_bytes"].is_null());
        let resized = issues
            .iter()
            .find(|v| v["status"] == "size_mismatch")
            .unwrap();
        assert_eq!(resized["oid"], oid(b"second"));
        assert_eq!(resized["expected_size_bytes"], 6);
        assert_eq!(resized["actual_size_bytes"], 1);
        assert_eq!(
            audit(&client, &base, "?q=absent").await["pagination"]["total"],
            0
        );
        assert_eq!(
            audit(&client, &base, "?offset=999").await["issues"],
            serde_json::json!([])
        );
        for query in [
            "?limit=0",
            "?limit=101",
            "?limit=1&limit=2",
            "?offset=-1",
            "?unknown=1",
        ] {
            let response = client
                .get(format!("{base}/api/v1/storage/audit{query}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400);
            assert_eq!(
                response_json(response).await["error"]["code"],
                "invalid_query"
            );
        }
        let response = client
            .post(format!("{base}/api/v1/storage/audit"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 405);
        assert_eq!(response.headers()["allow"], "GET");
        upload(&client, &base, b"first").await;
        upload(&client, &base, b"second").await;
        assert_eq!(audit(&client, &base, "").await["pagination"]["total"], 0);
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn imports_existing_files_but_rejected_uploads_do_not_create_expectations()
 {
    let root = tempfile::tempdir().unwrap();
    let path = object_path(root.path(), b"legacy");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"legacy").unwrap();
    let (base, task) = start(root.path(), false).await;
    let client = Client::builder().no_proxy().build().unwrap();
    assert_eq!(audit(&client, &base, "").await["tracked_objects"], 1);
    // A bad SHA-256 must fail before committing a file or inventory record.
    let result = client
        .put(format!("{base}/api/org/repo/object/{}", oid(b"expected")))
        .body("different")
        .send()
        .await;
    assert!(result.is_err() || !result.unwrap().status().is_success());
    assert_eq!(audit(&client, &base, "").await["tracked_objects"], 1);
    std::fs::remove_file(path).unwrap();
    assert_eq!(
        audit(&client, &base, "").await["issues"][0]["status"],
        "missing"
    );
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn database_failure_returns_no_partial_audit_or_raw_details() {
    let root = tempfile::tempdir().unwrap();
    let (base, task) = start(root.path(), false).await;
    let connection =
        rusqlite::Connection::open(root.path().join("inventory.sqlite3"))
            .unwrap();
    connection.execute("DROP TABLE objects", []).unwrap();
    let client = Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("{base}/api/v1/storage/audit"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let body = response.text().await.unwrap();
    let value: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["error"]["code"], "storage_unavailable");
    assert!(value.get("issues").is_none());
    assert!(!body.contains("objects"));
    assert!(!body.contains(root.path().to_str().unwrap()));
    // No successful acknowledgment when the inventory commit fails.
    let result = client
        .put(format!("{base}/api/org/repo/object/{}", oid(b"new")))
        .body("new")
        .send()
        .await;
    assert!(result.is_err() || !result.unwrap().status().is_success());
    task.abort();
}

#[tokio::test]
async fn corrupt_or_newer_database_blocks_startup_without_recreating_it() {
    for corrupt in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("inventory.sqlite3");
        if corrupt {
            std::fs::write(&path, b"not a database").unwrap();
        } else {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.pragma_update(None, "user_version", 99).unwrap();
        }
        let result = LocalServerBuilder::new(root.path().into())
            .spawn("127.0.0.1:0".parse().unwrap())
            .await;
        assert!(result.is_err());
        if corrupt {
            assert_eq!(std::fs::read(&path).unwrap(), b"not a database");
        } else {
            assert_eq!(
                rusqlite::Connection::open(path)
                    .unwrap()
                    .pragma_query_value::<u32, _>(None, "user_version", |r| r
                        .get(0))
                    .unwrap(),
                99
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_inventory_returns_limit_error_without_partial_issues() {
    let root = tempfile::tempdir().unwrap();
    let (base, task) = start(root.path(), false).await;
    let connection =
        rusqlite::Connection::open(root.path().join("inventory.sqlite3"))
            .unwrap();
    connection.execute_batch("WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n < 100001) INSERT INTO objects SELECT 'org', 'repo', printf('%064x', n), 1 FROM numbers;").unwrap();
    let client = Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("{base}/api/v1/storage/audit"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let body = response_json(response).await;
    assert_eq!(body["error"]["code"], "inventory_limit_exceeded");
    assert!(body.get("issues").is_none());
    task.abort();
}
