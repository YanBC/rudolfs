//! Public discovery API contract, including persistence and cache independence.
use rudolfs::LocalServerBuilder;
use serde_json::{Value, json};
use sha2::Digest;

#[tokio::test(flavor = "multi_thread")]
async fn repository_api_lists_permanent_objects_after_restart() {
    let data = tempfile::tempdir().unwrap();
    let mut builder = LocalServerBuilder::new(data.path().into());
    builder.key([42; 32]);
    let server = builder.spawn("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", server.addr());
    let task = tokio::spawn(server);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let empty: Value = client
        .get(format!("{base}/api/v1/repositories"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .map(|b| serde_json::from_slice(&b).unwrap())
        .unwrap();
    assert_eq!(empty["pagination"]["total"], 0);
    for (namespace, data) in [
        ("beta/assets", "hello"),
        ("alpha/art", "world!"),
        ("alpha/art", "more"),
    ] {
        let oid = hex::encode(sha2::Sha256::digest(data.as_bytes()));
        let response = client
            .put(format!("{base}/api/{namespace}/object/{oid}"))
            .body(data)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
    }
    task.abort();
    let mut builder = LocalServerBuilder::new(data.path().into());
    builder.key([42; 32]);
    let server = builder.spawn("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = format!("http://{}", server.addr());
    let task = tokio::spawn(server);
    let response = client
        .get(format!("{base}/api/v1/repositories?limit=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["content-type"], "application/json");
    let page: Value = response
        .bytes()
        .await
        .map(|b| serde_json::from_slice(&b).unwrap())
        .unwrap();
    assert_eq!(page["pagination"]["total"], 2);
    assert_eq!(page["repositories"][0]["organization"], "alpha");
    assert_eq!(page["repositories"][0]["object_count"], 2);
    assert_eq!(page["repositories"][0]["size_bytes"], 10);
    assert_eq!(
        page["repositories"][0]["lfs_url"],
        format!("{base}/api/alpha/art")
    );
    let page: Value = client
        .get(format!(
            "{base}{}",
            page["pagination"]["next"].as_str().unwrap()
        ))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .map(|b| serde_json::from_slice(&b).unwrap())
        .unwrap();
    assert_eq!(page["repositories"][0]["organization"], "beta");
    assert!(page["pagination"]["next"].is_null());
    for (query, count) in
        [("q=ALPHA%2FART", 1), ("q=missing", 0), ("offset=999", 0)]
    {
        let page: Value = client
            .get(format!("{base}/api/v1/repositories?{query}"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .map(|b| serde_json::from_slice(&b).unwrap())
            .unwrap();
        assert_eq!(page["repositories"].as_array().unwrap().len(), count);
    }
    for query in [
        "limit=0",
        "limit=101",
        "offset=-1",
        "offset=abc",
        "limit=1&limit=2",
        "unknown=x",
    ] {
        let response = client
            .get(format!("{base}/api/v1/repositories?{query}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.bytes().await.unwrap())
                .unwrap()["error"]["code"],
            "invalid_query"
        );
    }
    let response = client
        .post(format!("{base}/api/v1/repositories"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 405);
    assert_eq!(response.headers()["allow"], "GET");
    let spec: Value = client
        .get(format!("{base}/api/v1/openapi.json"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .map(|b| serde_json::from_slice(&b).unwrap())
        .unwrap();
    assert_eq!(spec["openapi"], "3.1.0");
    assert_eq!(
        spec["paths"]["/api/v1/repositories"]["get"]["operationId"],
        "listRepositories"
    );
    // Existing Git LFS route remains usable.
    let response = client
        .post(format!("{base}/api/alpha/art/objects/batch"))
        .body(json!({"operation":"download", "objects":[]}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    task.abort();
}
