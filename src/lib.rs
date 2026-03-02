use napi::bindgen_prelude::*;
use napi_derive::napi;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, DbTransaction, WriteBatch};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static WO_DURABLE: WriteOptions = WriteOptions {
    await_durable: true,
};
static WO_NON_DURABLE: WriteOptions = WriteOptions {
    await_durable: false,
};

/// Resolve an object store from a URL string.
///
/// Supported schemes:
///   `:memory:`       — in-memory (default)
///   `file:///path`   — local filesystem
///   `s3://bucket`    — AWS S3, Cloudflare R2, MinIO (reads AWS_* env vars)
///   `az://container` — Azure Blob (reads AZURE_* env vars)
fn resolve_store(url: &str) -> Result<Arc<dyn ObjectStore>> {
    if url.is_empty() || url == ":memory:" {
        Ok(Arc::new(InMemory::new()))
    } else if url.starts_with("s3://") {
        use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
        let bucket = url.strip_prefix("s3://").unwrap().trim_end_matches('/');
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map_err(|e| Error::from_reason(format!("failed to build S3 store: {e}")))?;
        Ok(Arc::new(store))
    } else if url.starts_with("az://") || url.starts_with("azure://") {
        Db::resolve_object_store(url)
            .map_err(|e| Error::from_reason(format!("failed to resolve Azure store: {e}")))
    } else if url.starts_with("file://") {
        let path = url.strip_prefix("file://").unwrap();
        let lfs = LocalFileSystem::new_with_prefix(path)
            .map_err(|e| Error::from_reason(format!("failed to create local store: {e}")))?;
        Ok(Arc::new(lfs))
    } else {
        Db::resolve_object_store(url)
            .map_err(|e| Error::from_reason(format!("failed to resolve store: {e}")))
    }
}

fn to_napi_err(e: slatedb::Error) -> Error {
    Error::from_reason(format!("{e}"))
}

// ---------------------------------------------------------------------------
// KeyValue — returned from scan
// ---------------------------------------------------------------------------
#[napi(object)]
pub struct KeyValue {
    pub key: Buffer,
    pub value: Buffer,
}

// ---------------------------------------------------------------------------
// IsolationLevel enum
// ---------------------------------------------------------------------------
#[napi]
pub enum JsIsolationLevel {
    Snapshot,
    SerializableSnapshot,
}

// ---------------------------------------------------------------------------
// SlateDB class
// ---------------------------------------------------------------------------
#[napi]
pub struct SlateDB {
    db: Db,
}

#[napi]
impl SlateDB {
    /// Open a SlateDB instance.
    /// Times out after 30 seconds on misconfigured backends.
    #[napi(factory)]
    pub async fn open(path: String, url: Option<String>) -> Result<SlateDB> {
        let url = url.unwrap_or_else(|| ":memory:".to_string());
        let store = resolve_store(&url)?;
        let db = tokio::time::timeout(Duration::from_secs(30), Db::open(path.as_str(), store))
            .await
            .map_err(|_| {
                Error::from_reason(
                    "SlateDB.open timed out after 30s — check credentials, region, and bucket access",
                )
            })?
            .map_err(to_napi_err)?;
        Ok(SlateDB { db })
    }

    /// Put a key-value pair.
    #[napi]
    pub async unsafe fn put(
        &mut self,
        key: Buffer,
        value: Buffer,
        await_durable: Option<bool>,
    ) -> Result<()> {
        let opts = if await_durable.unwrap_or(true) {
            &WO_DURABLE
        } else {
            &WO_NON_DURABLE
        };
        self.db
            .put_with_options(&key[..], &value[..], &PutOptions::default(), opts)
            .await
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Get raw bytes by key. Returns null if not found.
    #[napi]
    pub async fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        match self.db.get(&key[..]).await.map_err(to_napi_err)? {
            Some(val) => Ok(Some(Buffer::from(val.to_vec()))),
            None => Ok(None),
        }
    }

    /// Get value as UTF-8 string. Returns null if not found.
    #[napi(js_name = "getString")]
    pub async fn get_string(&self, key: Buffer) -> Result<Option<String>> {
        match self.db.get(&key[..]).await.map_err(to_napi_err)? {
            Some(val) => {
                let s = String::from_utf8(val.to_vec())
                    .map_err(|e| Error::from_reason(format!("invalid UTF-8: {e}")))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Delete a key.
    #[napi]
    pub async unsafe fn delete(
        &mut self,
        key: Buffer,
        await_durable: Option<bool>,
    ) -> Result<()> {
        let opts = if await_durable.unwrap_or(true) {
            &WO_DURABLE
        } else {
            &WO_NON_DURABLE
        };
        self.db
            .delete_with_options(&key[..], opts)
            .await
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Flush the memtable to object storage.
    #[napi]
    pub async fn flush(&self) -> Result<()> {
        self.db.flush().await.map_err(to_napi_err)
    }

    /// Range scan [start, end). Both bounds optional (full scan if omitted).
    #[napi]
    pub async fn scan(
        &self,
        start: Option<Buffer>,
        end: Option<Buffer>,
    ) -> Result<Vec<KeyValue>> {
        let lo = match &start {
            Some(b) => Bound::Included(b.to_vec()),
            None => Bound::Unbounded,
        };
        let hi = match &end {
            Some(b) => Bound::Excluded(b.to_vec()),
            None => Bound::Unbounded,
        };

        let mut iter = self.db.scan((lo, hi)).await.map_err(to_napi_err)?;
        let mut results = Vec::new();

        while let Some(kv) = iter.next().await.map_err(to_napi_err)? {
            results.push(KeyValue {
                key: Buffer::from(kv.key.to_vec()),
                value: Buffer::from(kv.value.to_vec()),
            });
        }

        Ok(results)
    }

    /// Write a batch atomically.
    #[napi(js_name = "writeBatch")]
    pub async unsafe fn write_batch(
        &mut self,
        batch: &JsWriteBatch,
        await_durable: Option<bool>,
    ) -> Result<()> {
        let opts = if await_durable.unwrap_or(true) {
            &WO_DURABLE
        } else {
            &WO_NON_DURABLE
        };
        let wb = {
            let mut guard = batch.inner.lock().await;
            guard.take().ok_or_else(|| Error::from_reason("WriteBatch already consumed"))?
        };
        self.db
            .write_with_options(wb, opts)
            .await
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Begin an ACID transaction.
    #[napi]
    pub async fn begin(
        &self,
        isolation: Option<JsIsolationLevel>,
    ) -> Result<JsTransaction> {
        let level = match isolation.unwrap_or(JsIsolationLevel::Snapshot) {
            JsIsolationLevel::Snapshot => slatedb::IsolationLevel::Snapshot,
            JsIsolationLevel::SerializableSnapshot => {
                slatedb::IsolationLevel::SerializableSnapshot
            }
        };
        let txn = self.db.begin(level).await.map_err(to_napi_err)?;
        Ok(JsTransaction {
            inner: Mutex::new(Some(txn)),
        })
    }

    /// Close the database and free native resources.
    #[napi]
    pub async unsafe fn close(&mut self) -> Result<()> {
        self.db.close().await.map_err(to_napi_err)
    }
}

// ---------------------------------------------------------------------------
// WriteBatch class
// ---------------------------------------------------------------------------
#[napi(js_name = "WriteBatch")]
pub struct JsWriteBatch {
    inner: Mutex<Option<WriteBatch>>,
}

#[napi]
impl JsWriteBatch {
    #[napi(constructor)]
    pub fn new() -> Self {
        JsWriteBatch {
            inner: Mutex::new(Some(WriteBatch::new())),
        }
    }

    /// Add a put to the batch.
    #[napi]
    pub async fn put(&self, key: Buffer, value: Buffer) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let wb = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("WriteBatch already consumed"))?;
        wb.put(&key[..], &value[..]);
        Ok(())
    }

    /// Add a delete to the batch.
    #[napi]
    pub async fn delete(&self, key: Buffer) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let wb = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("WriteBatch already consumed"))?;
        wb.delete(&key[..]);
        Ok(())
    }

    /// Free the batch without writing.
    #[napi]
    pub async fn free(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        *guard = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transaction class
// ---------------------------------------------------------------------------
#[napi(js_name = "Transaction")]
pub struct JsTransaction {
    inner: Mutex<Option<DbTransaction>>,
}

#[napi]
impl JsTransaction {
    /// Put within the transaction.
    #[napi]
    pub async fn put(&self, key: Buffer, value: Buffer) -> Result<()> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        txn.put(&key[..], &value[..]).map_err(to_napi_err)
    }

    /// Get within the transaction. Returns null if not found.
    #[napi]
    pub async fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        match txn.get(&key[..]).await.map_err(to_napi_err)? {
            Some(val) => Ok(Some(Buffer::from(val.to_vec()))),
            None => Ok(None),
        }
    }

    /// Get within the transaction as UTF-8 string.
    #[napi(js_name = "getString")]
    pub async fn get_string(&self, key: Buffer) -> Result<Option<String>> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        match txn.get(&key[..]).await.map_err(to_napi_err)? {
            Some(val) => {
                let s = String::from_utf8(val.to_vec())
                    .map_err(|e| Error::from_reason(format!("invalid UTF-8: {e}")))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Delete within the transaction.
    #[napi]
    pub async fn delete(&self, key: Buffer) -> Result<()> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        txn.delete(&key[..]).map_err(to_napi_err)
    }

    /// Commit the transaction.
    #[napi]
    pub async fn commit(&self, await_durable: Option<bool>) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        let opts = if await_durable.unwrap_or(true) {
            &WO_DURABLE
        } else {
            &WO_NON_DURABLE
        };
        txn.commit_with_options(opts).await.map_err(to_napi_err)?;
        Ok(())
    }

    /// Rollback (abort) the transaction.
    #[napi]
    pub async fn rollback(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        txn.rollback();
        Ok(())
    }
}
