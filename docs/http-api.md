# HTTP API

Rudolfs provides web features and equivalent HTTP APIs. The web repository
browser uses this API directly. Contributors must follow [the project policy](../AGENTS.md).
The machine-readable contract is [OpenAPI](openapi.json), also served at
`GET /api/v1/openapi.json`.

## List repositories

```sh
curl --fail --get 'http://localhost:8080/api/v1/repositories' \
  --data-urlencode 'q=studio' --data-urlencode 'limit=25' --data-urlencode 'offset=0'
```

All parameters are optional. `q` is a case-insensitive substring of
`organization/repository` (maximum 200 UTF-8 bytes). `limit` defaults to 25 and
must be 1–100; `offset` defaults to 0 and must be a nonnegative platform-sized
integer. Duplicate and unknown parameters are rejected. Query strings are capped
at 2,048 bytes. Use standard URL encoding.

```json
{
  "repositories": [
    {
      "organization": "studio",
      "repository": "assets",
      "object_count": 12,
      "size_bytes": 4194304,
      "lfs_url": "http://localhost:8080/api/studio/assets"
    }
  ],
  "pagination": {"limit": 25, "offset": 0, "total": 1, "next": null}
}
```

Results are sorted by organization then repository in ASCII order. `total` is
the number of matching namespaces. Follow `pagination.next` (a relative URL)
until null. An empty inventory, unmatched search, or offset beyond the final
result returns HTTP 200 with an empty `repositories` array.

Only namespaces containing persisted LFS objects appear. These are not hosted
Git repositories: branches, code, and file names are unavailable. Counts are
object counts, and bytes are stored object bytes, not Git working tree sizes.
Repositories with no LFS objects do not appear.

Local storage is scanned from disk; symlink directories are not traversed. S3
uses paginated ListObjectsV2 scoped to the configured bucket/prefix and requires
`s3:ListBucket` permission for that prefix. OSS uses authenticated Swift container
JSON listing with prefix and marker pagination, and requires container-list
permission. Encrypted and cached backends enumerate permanent objects, never
only cached objects. Existing data is discovered after restart without an index.

Every request scans inventory anew before paginating the result. A scan is
limited to 100,000 recognized objects and 30 seconds; it is intended for modest
inventories, not an indexed analytics service. Larger deployments need a future
persistent index. Pages are not a snapshot: concurrent uploads/deletes can change
counts and page boundaries. Refresh/restart pagination after changes.

## Errors and access

Errors use `{"error":{"code":"invalid_query","message":"..."}}`:

| HTTP status | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_query` | Invalid, unknown, or duplicate query parameter |
| 405 | `method_not_allowed` | Unsupported method (`Allow: GET`) |
| 503 | `storage_unavailable` | Backend scan failed, including missing list permission |
| 503 | `inventory_limit_exceeded` | Scan exceeded the inventory size limit |
| 503 | `inventory_timeout` | Scan exceeded 30 seconds |

Failures never return partial listings or raw backend errors. Responses use
`application/json` and `Cache-Control: no-store`. Discovery and OpenAPI routes
accept GET; OpenAPI does not accept query parameters. Existing Git LFS protocol
routes are unchanged. Reverse proxies must supply trusted forwarded host/proto
headers when generating externally reachable `lfs_url` values.

Rudolfs has **no built-in authentication or authorization**. Inventory is visible
to anyone able to reach the server, just like the web UI. Protect all HTTP routes
with trusted-network controls or an authenticating reverse proxy.
