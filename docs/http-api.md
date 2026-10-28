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

## Local storage audit

`GET /api/v1/storage/audit` compares the local backend's SQLite inventory with
current permanent files. The home page's Storage health panel uses this API.
It accepts the same `limit`, `offset`, and `q` parameters and validation rules as
repository discovery. Issues sort by organization, repository, then object ID.
`pagination.total` counts matching issues; `tracked_objects` counts all inventory
records before filtering. Empty inventories return zero tracked objects and no
issues. A healthy tracked inventory returns no issues. Missing data is a
successful audit (HTTP 200 with issues), not an HTTP error.

```json
{
  "tracked_objects": 1,
  "issues": [{
    "organization": "studio",
    "repository": "assets",
    "oid": "0000000000000000000000000000000000000000000000000000000000000000",
    "status": "missing",
    "expected_size_bytes": 4096,
    "actual_size_bytes": null
  }],
  "pagination": {"limit": 25, "offset": 0, "total": 1, "next": null}
}
```

`status` is `missing` (no recognized regular file) or `size_mismatch` (different
stored byte length, with `actual_size_bytes` populated). No filesystem paths or
backend error details are exposed. No SHA-256 verification is performed by this
audit; same-size corruption remains detectable through normal verified downloads.
There is no automatic audit scheduler, alert delivery, deletion, or recovery.
Re-uploading valid contents repairs a missing or size-mismatched object.

HTTP 400 uses `invalid_query`; 405 uses `method_not_allowed` with `Allow: GET`.
S3 and OSS return HTTP 409 `audit_unavailable`, including when they use a disk
cache. A cache is never treated as permanent inventory. Database/scan errors
return HTTP 503 `storage_unavailable`; exceeding 100,000 tracked records or
100,000 scanned files returns `inventory_limit_exceeded`; scans exceeding
30 seconds return `inventory_timeout`. No partial issues are returned on failure.
Responses have `Cache-Control: no-store`. Each page rescans storage; concurrent
uploads and file changes can cause transient findings and shift page boundaries.
Run audits again after writes settle. Protect this route with the same deployment
access controls as the UI and other APIs.

### Inventory lifecycle and backups

The local backend creates `<data-root>/inventory.sqlite3` automatically. SQLite
runs in WAL mode with a busy timeout and full synchronous commits, on blocking
workers rather than the async executor. Successful uploads are acknowledged only
after the file is stored and its inventory record commits. File storage and SQLite
are not one atomic transaction: a crash or database error between these steps can
leave an untracked file. Restarting imports unknown files, repairing that gap.
An upload whose database write fails returns an error even if its file was stored.
Startup fails on an unreadable/corrupt database or unsupported schema version;
it never silently recreates a damaged database.

Startup imports all recognized existing files, adding only previously unknown
object IDs. It never deletes expectations for missing files or overwrites expected
sizes, so restarting cannot hide a deletion or size change. An interrupted import
resumes on the next startup. Files added externally while running are imported on
restart. Import establishes a baseline from what is present; it cannot detect
files deleted before the first inventory was created. Discovery remains a scan of
currently stored files, while the audit shows losses relative to SQLite.

Keep the database on writable local storage supporting SQLite file locks; avoid
sharing the data root over a filesystem without reliable locking. Back up both
the objects and inventory. Stop all writers before taking a filesystem backup,
including any `inventory.sqlite3-wal` and `inventory.sqlite3-shm` files present,
or use SQLite's backup API for the database and coordinate the object snapshot.
Never delete the database to clear warnings: that discards the history needed to
detect missing files. The inventory does not contain file contents and is not a
replacement for object backups. No automatic garbage collection removes records.
