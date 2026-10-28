//! Durable expectations for permanent local storage. Never used for caches.
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, params};

use super::{Namespace, StorageKey};

#[derive(Clone)]
pub struct Inventory(Arc<Mutex<Connection>>);

impl Inventory {
    pub async fn open(path: PathBuf) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let connection =
                Connection::open(path).map_err(io::Error::other)?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(io::Error::other)?;
            let version: u32 = connection
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .map_err(io::Error::other)?;
            if version > 1 {
                return Err(io::Error::other(
                    "unsupported inventory database version",
                ));
            }
            connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;").map_err(io::Error::other)?;
            if version == 0 {
                connection.execute_batch(
                    "BEGIN IMMEDIATE;
                     CREATE TABLE IF NOT EXISTS objects (
                        organization TEXT NOT NULL,
                        repository TEXT NOT NULL,
                        oid TEXT NOT NULL CHECK(length(oid) = 64),
                        size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                        PRIMARY KEY (organization, repository, oid)
                     ) WITHOUT ROWID;
                     PRAGMA user_version=1;
                     COMMIT;"
                ).map_err(io::Error::other)?;
            }
            // An established but damaged inventory must never be silently reset.
            connection.prepare("SELECT organization, repository, oid, size_bytes FROM objects LIMIT 0").map_err(io::Error::other)?;
            let integrity: String = connection.pragma_query_value(None, "quick_check", |row| row.get(0)).map_err(io::Error::other)?;
            if integrity != "ok" {
                return Err(io::Error::other("inventory database integrity check failed"));
            }
            Ok(Self(Arc::new(Mutex::new(connection))))
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn with_connection<T, F>(&self, operation: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> io::Result<T> + Send + 'static,
    {
        let connection = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = connection.lock().map_err(|_| {
                io::Error::other("inventory database lock failed")
            })?;
            operation(&mut guard)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Startup imports only unknown objects; it must not bless changed sizes.
    pub async fn record(
        &self,
        entries: Vec<(StorageKey, u64)>,
        uploaded: bool,
    ) -> io::Result<()> {
        self.with_connection(move |connection| {
            let transaction = connection.transaction().map_err(io::Error::other)?;
            {
                let sql = if uploaded {
                    "INSERT INTO objects VALUES (?1, ?2, ?3, ?4) ON CONFLICT (organization, repository, oid) DO UPDATE SET size_bytes=excluded.size_bytes"
                } else {
                    "INSERT INTO objects VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING"
                };
                let mut statement = transaction.prepare(sql).map_err(io::Error::other)?;
                for (key, size) in entries {
                    let size = i64::try_from(size).map_err(io::Error::other)?;
                    statement.execute(params![key.namespace().org(), key.namespace().project(), key.oid().to_string(), size]).map_err(io::Error::other)?;
                }
            }
            transaction.commit().map_err(io::Error::other)
        }).await
    }

    /// One extra entry allows the HTTP layer to reject oversized audits.
    pub async fn entries(&self) -> io::Result<Vec<(StorageKey, u64)>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT organization, repository, oid, size_bytes FROM objects ORDER BY organization, repository, oid LIMIT 100001").map_err(io::Error::other)?;
            let mut rows = statement.query([]).map_err(io::Error::other)?;
            let mut entries = Vec::new();
            while let Some(row) = rows.next().map_err(io::Error::other)? {
                let org: String = row.get(0).map_err(io::Error::other)?;
                let repo: String = row.get(1).map_err(io::Error::other)?;
                let oid: String = row.get(2).map_err(io::Error::other)?;
                let size: i64 = row.get(3).map_err(io::Error::other)?;
                let namespace = Namespace::new(org, repo);
                if !namespace.is_valid() || size < 0 {
                    return Err(io::Error::other("invalid inventory record"));
                }
                let key = StorageKey::new(namespace, oid.parse().map_err(io::Error::other)?);
                entries.push((key, size as u64));
            }
            Ok(entries)
        }).await
    }
}
