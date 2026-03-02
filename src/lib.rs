use bytes::Bytes;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, DbReaderOptions, DurabilityLevel, FlushOptions, FlushType,
    MergeOptions, PutOptions, ReadOptions, ScanOptions, Settings, Ttl, WriteOptions,
};
use slatedb::{Db, DbIterator, DbReader, DbTransaction, WriteBatch};
use slatedb::{MergeOperator, MergeOperatorError};
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

fn wo(await_durable: Option<bool>) -> &'static WriteOptions {
    if await_durable.unwrap_or(true) {
        &WO_DURABLE
    } else {
        &WO_NON_DURABLE
    }
}

fn build_put_options(ttl: Option<u64>) -> PutOptions {
    PutOptions {
        ttl: match ttl {
            None => Ttl::Default,
            Some(0) => Ttl::NoExpiry,
            Some(ms) => Ttl::ExpireAfter(ms),
        },
    }
}

fn build_read_options(read_level: Option<JsDurabilityLevel>) -> ReadOptions {
    let mut opts = ReadOptions::default();
    if let Some(level) = read_level {
        opts.durability_filter = match level {
            JsDurabilityLevel::Memory => DurabilityLevel::Memory,
            JsDurabilityLevel::Remote => DurabilityLevel::Remote,
        };
    }
    opts
}

fn build_scan_options(
    read_level: Option<JsDurabilityLevel>,
    read_ahead_bytes: Option<u32>,
    max_fetch_tasks: Option<u32>,
) -> ScanOptions {
    let mut opts = ScanOptions::default();
    if let Some(level) = read_level {
        opts.durability_filter = match level {
            JsDurabilityLevel::Memory => DurabilityLevel::Memory,
            JsDurabilityLevel::Remote => DurabilityLevel::Remote,
        };
    }
    if let Some(n) = read_ahead_bytes {
        opts.read_ahead_bytes = n as usize;
    }
    if let Some(n) = max_fetch_tasks {
        opts.max_fetch_tasks = n as usize;
    }
    opts
}

fn build_flush_options(flush_type: Option<JsFlushType>) -> FlushOptions {
    FlushOptions {
        flush_type: match flush_type.unwrap_or(JsFlushType::Wal) {
            JsFlushType::MemTable => FlushType::MemTable,
            JsFlushType::Wal => FlushType::Wal,
        },
    }
}

fn make_range(start: &Option<Buffer>, end: &Option<Buffer>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let lo = match start {
        Some(b) => Bound::Included(b.to_vec()),
        None => Bound::Unbounded,
    };
    let hi = match end {
        Some(b) => Bound::Excluded(b.to_vec()),
        None => Bound::Unbounded,
    };
    (lo, hi)
}

async fn collect_iter(mut iter: DbIterator) -> Result<Vec<KeyValue>> {
    let mut results = Vec::new();
    while let Some(kv) = iter.next().await.map_err(to_napi_err)? {
        results.push(KeyValue {
            key: Buffer::from(kv.key.to_vec()),
            value: Buffer::from(kv.value.to_vec()),
        });
    }
    Ok(results)
}

/// Resolve an object store from a URL string.
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

/// Quick probe to verify credentials/access before opening the database.
/// Issues a HEAD request with a short timeout. Auth errors (403/401) surface
/// in ~1-2s instead of hanging for 30s in SlateDB's infinite-retry loop.
/// NotFound is expected (the probe path doesn't exist) and means access is OK.
async fn probe_store(store: &Arc<dyn ObjectStore>) -> Result<()> {
    use object_store::path::Path as ObjPath;
    let probe_path = ObjPath::from("__slatedb_probe__");
    let probe = store.head(&probe_path);
    match tokio::time::timeout(Duration::from_secs(5), probe).await {
        Ok(Ok(_)) => Ok(()), // path exists (unlikely)
        Ok(Err(object_store::Error::NotFound { .. })) => Ok(()), // expected — access OK
        Ok(Err(e)) => Err(Error::from_reason(format!(
            "store access check failed: {e}"
        ))),
        Err(_) => Err(Error::from_reason(
            "store access check timed out after 5s — check credentials, region, and endpoint",
        )),
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
// CheckpointResult — returned from createCheckpoint
// ---------------------------------------------------------------------------
#[napi(object)]
pub struct JsCheckpointResult {
    pub id: String,
    pub manifest_id: i64,
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------
#[napi]
pub enum JsIsolationLevel {
    Snapshot,
    SerializableSnapshot,
}

#[napi]
pub enum JsDurabilityLevel {
    Memory,
    Remote,
}

#[napi]
pub enum JsFlushType {
    MemTable,
    Wal,
}

#[napi]
pub enum JsCheckpointScope {
    All,
    Durable,
}

// ---------------------------------------------------------------------------
// Built-in merge operators
// ---------------------------------------------------------------------------

/// A string-concatenation merge operator (for testing / simple use cases).
struct StringConcatMergeOperator;

impl MergeOperator for StringConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> std::result::Result<Bytes, MergeOperatorError> {
        let mut result = existing_value.unwrap_or_default().to_vec();
        result.extend_from_slice(&value);
        Ok(Bytes::from(result))
    }
}

/// A u64 counter merge operator — each merge operand is an 8-byte LE u64 to add.
struct Uint64AddMergeOperator;

impl MergeOperator for Uint64AddMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> std::result::Result<Bytes, MergeOperatorError> {
        let existing = existing_value
            .and_then(|v| v.as_ref().try_into().ok().map(u64::from_le_bytes))
            .unwrap_or(0);
        let add = value
            .as_ref()
            .try_into()
            .ok()
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        Ok(Bytes::copy_from_slice(&(existing + add).to_le_bytes()))
    }
}

// ---------------------------------------------------------------------------
// Settings object — JS-friendly subset of slatedb::config::Settings
// ---------------------------------------------------------------------------
#[napi(object)]
pub struct JsSettings {
    /// Flush interval in milliseconds. Null = disable automatic flushing.
    pub flush_interval_ms: Option<u32>,
    /// Min memtable size before flush to L0 (bytes).
    pub l0_sst_size_bytes: Option<u32>,
    /// Max number of L0 SSTs before backpressure.
    pub l0_max_ssts: Option<u32>,
    /// Max unflushed bytes before writer backpressure.
    pub max_unflushed_bytes: Option<u32>,
    /// Default TTL for put operations (milliseconds). Null = no expiry.
    pub default_ttl_ms: Option<u32>,
    /// Merge operator: "string_concat" or "uint64_add". Null = no merge support.
    pub merge_operator: Option<String>,
    /// Open in read-only mode. Multiple readers can access the same DB concurrently.
    pub read_only: Option<bool>,
}

// ---------------------------------------------------------------------------
// SlateDB class
// ---------------------------------------------------------------------------
enum DbKind {
    Writer(Db),
    Reader(Box<DbReader>),
}

#[napi]
pub struct SlateDB {
    inner: DbKind,
}

impl SlateDB {
    fn db(&self) -> Result<&Db> {
        match &self.inner {
            DbKind::Writer(db) => Ok(db),
            DbKind::Reader(_) => Err(Error::from_reason(
                "operation not supported in read-only mode",
            )),
        }
    }

    fn db_mut(&mut self) -> Result<&mut Db> {
        match &mut self.inner {
            DbKind::Writer(db) => Ok(db),
            DbKind::Reader(_) => Err(Error::from_reason(
                "operation not supported in read-only mode",
            )),
        }
    }
}

#[napi]
impl SlateDB {
    /// Open a SlateDB instance. Pass optional settings to configure flush
    /// interval, SST sizes, TTL, merge operators, read-only mode, etc.
    #[napi(factory)]
    pub async fn open(
        path: String,
        url: Option<String>,
        settings: Option<JsSettings>,
    ) -> Result<SlateDB> {
        let url = url.unwrap_or_else(|| ":memory:".to_string());
        let store = resolve_store(&url)?;

        // Quick probe for cloud backends — catches auth errors immediately
        // instead of hanging in SlateDB's infinite-retry loop.
        if url.starts_with("s3://") || url.starts_with("az://") || url.starts_with("gs://") {
            probe_store(&store).await?;
        }

        let read_only = settings.as_ref().and_then(|s| s.read_only).unwrap_or(false);

        // Read-only mode — open a DbReader (no fencing, multiple allowed)
        if read_only {
            let reader = tokio::time::timeout(
                Duration::from_secs(30),
                DbReader::open(path.as_str(), store, None, DbReaderOptions::default()),
            )
            .await
            .map_err(|_| Error::from_reason("SlateDB.open timed out"))?
            .map_err(to_napi_err)?;
            return Ok(SlateDB {
                inner: DbKind::Reader(Box::new(reader)),
            });
        }

        // Writer mode
        let inner = match settings {
            Some(settings) => {
                let mut cfg = Settings::default();
                if let Some(ms) = settings.flush_interval_ms {
                    cfg.flush_interval = Some(Duration::from_millis(ms as u64));
                }
                if let Some(n) = settings.l0_sst_size_bytes {
                    cfg.l0_sst_size_bytes = n as usize;
                }
                if let Some(n) = settings.l0_max_ssts {
                    cfg.l0_max_ssts = n as usize;
                }
                if let Some(n) = settings.max_unflushed_bytes {
                    cfg.max_unflushed_bytes = n as usize;
                }
                if let Some(ttl) = settings.default_ttl_ms {
                    cfg.default_ttl = Some(ttl as u64);
                }

                let merge_op: Option<Arc<dyn MergeOperator + Send + Sync>> =
                    match settings.merge_operator.as_deref() {
                        Some("string_concat") => Some(Arc::new(StringConcatMergeOperator)),
                        Some("uint64_add") => Some(Arc::new(Uint64AddMergeOperator)),
                        Some(other) => {
                            return Err(Error::from_reason(format!(
                        "unknown merge operator: '{other}'. Use 'string_concat' or 'uint64_add'"
                    )))
                        }
                        None => None,
                    };
                cfg.merge_operator = merge_op;

                let builder = Db::builder(path.as_str(), store).with_settings(cfg);
                DbKind::Writer(
                    tokio::time::timeout(Duration::from_secs(30), builder.build())
                        .await
                        .map_err(|_| Error::from_reason("SlateDB.open timed out"))?
                        .map_err(to_napi_err)?,
                )
            }
            None => DbKind::Writer(
                tokio::time::timeout(Duration::from_secs(30), Db::open(path.as_str(), store))
                    .await
                    .map_err(|_| Error::from_reason("SlateDB.open timed out"))?
                    .map_err(to_napi_err)?,
            ),
        };
        Ok(SlateDB { inner })
    }

    // -----------------------------------------------------------------------
    // Put / Delete
    // -----------------------------------------------------------------------

    /// Put a key-value pair.
    ///
    /// # Safety
    /// Called from JS via napi-rs. The `&mut self` borrow is safe because
    /// napi-rs prevents concurrent mutable access from JavaScript.
    #[napi]
    pub async unsafe fn put(
        &mut self,
        key: Buffer,
        value: Buffer,
        await_durable: Option<bool>,
        ttl: Option<u32>,
    ) -> Result<()> {
        self.db_mut()?
            .put_with_options(
                &key[..],
                &value[..],
                &build_put_options(ttl.map(|v| v as u64)),
                wo(await_durable),
            )
            .await
            .map_err(to_napi_err)
    }

    /// Delete a key.
    ///
    /// # Safety
    /// Called from JS via napi-rs. The `&mut self` borrow is safe because
    /// napi-rs prevents concurrent mutable access from JavaScript.
    #[napi]
    pub async unsafe fn delete(&mut self, key: Buffer, await_durable: Option<bool>) -> Result<()> {
        self.db_mut()?
            .delete_with_options(&key[..], wo(await_durable))
            .await
            .map_err(to_napi_err)
    }

    // -----------------------------------------------------------------------
    // Merge
    // -----------------------------------------------------------------------

    /// Merge a value into the database using the configured merge operator.
    /// Requires a merge operator to be set via settings in open().
    ///
    /// # Safety
    /// Called from JS via napi-rs. The `&mut self` borrow is safe because
    /// napi-rs prevents concurrent mutable access from JavaScript.
    #[napi]
    pub async unsafe fn merge(
        &mut self,
        key: Buffer,
        value: Buffer,
        await_durable: Option<bool>,
        ttl: Option<u32>,
    ) -> Result<()> {
        let merge_opts = MergeOptions {
            ttl: match ttl.map(|v| v as u64) {
                None => Ttl::Default,
                Some(0) => Ttl::NoExpiry,
                Some(ms) => Ttl::ExpireAfter(ms),
            },
        };
        self.db_mut()?
            .merge_with_options(&key[..], &value[..], &merge_opts, wo(await_durable))
            .await
            .map_err(to_napi_err)
    }

    // -----------------------------------------------------------------------
    // Get
    // -----------------------------------------------------------------------

    /// Get raw bytes by key. Returns null if not found.
    #[napi]
    pub async fn get(
        &self,
        key: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Option<Buffer>> {
        let opts = build_read_options(read_level);
        let result = match &self.inner {
            DbKind::Writer(db) => db.get_with_options(&key[..], &opts).await,
            DbKind::Reader(r) => r.get_with_options(&key[..], &opts).await,
        }
        .map_err(to_napi_err)?;
        match result {
            Some(val) => Ok(Some(Buffer::from(val.to_vec()))),
            None => Ok(None),
        }
    }

    /// Get value as UTF-8 string. Returns null if not found.
    #[napi(js_name = "getString")]
    pub async fn get_string(
        &self,
        key: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Option<String>> {
        let opts = build_read_options(read_level);
        let result = match &self.inner {
            DbKind::Writer(db) => db.get_with_options(&key[..], &opts).await,
            DbKind::Reader(r) => r.get_with_options(&key[..], &opts).await,
        }
        .map_err(to_napi_err)?;
        match result {
            Some(val) => {
                let s = String::from_utf8(val.to_vec())
                    .map_err(|e| Error::from_reason(format!("invalid UTF-8: {e}")))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // Scan
    // -----------------------------------------------------------------------

    /// Range scan [start, end). Both bounds optional (full scan if omitted).
    #[napi]
    pub async fn scan(
        &self,
        start: Option<Buffer>,
        end: Option<Buffer>,
        read_level: Option<JsDurabilityLevel>,
        read_ahead_bytes: Option<u32>,
        max_fetch_tasks: Option<u32>,
    ) -> Result<Vec<KeyValue>> {
        let opts = build_scan_options(read_level, read_ahead_bytes, max_fetch_tasks);
        let range = make_range(&start, &end);
        let iter = match &self.inner {
            DbKind::Writer(db) => db.scan_with_options(range, &opts).await,
            DbKind::Reader(r) => r.scan_with_options(range, &opts).await,
        }
        .map_err(to_napi_err)?;
        collect_iter(iter).await
    }

    /// Prefix scan — returns all keys starting with the given prefix.
    #[napi(js_name = "scanPrefix")]
    pub async fn scan_prefix(
        &self,
        prefix: Buffer,
        read_level: Option<JsDurabilityLevel>,
        read_ahead_bytes: Option<u32>,
        max_fetch_tasks: Option<u32>,
    ) -> Result<Vec<KeyValue>> {
        let opts = build_scan_options(read_level, read_ahead_bytes, max_fetch_tasks);
        let iter = match &self.inner {
            DbKind::Writer(db) => db.scan_prefix_with_options(&prefix[..], &opts).await,
            DbKind::Reader(r) => r.scan_prefix_with_options(&prefix[..], &opts).await,
        }
        .map_err(to_napi_err)?;
        collect_iter(iter).await
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Flush to object storage.
    #[napi]
    pub async fn flush(&self, flush_type: Option<JsFlushType>) -> Result<()> {
        self.db()?
            .flush_with_options(build_flush_options(flush_type))
            .await
            .map_err(to_napi_err)
    }

    // -----------------------------------------------------------------------
    // WriteBatch
    // -----------------------------------------------------------------------

    /// Write a batch atomically.
    ///
    /// # Safety
    /// Called from JS via napi-rs. The `&mut self` borrow is safe because
    /// napi-rs prevents concurrent mutable access from JavaScript.
    #[napi(js_name = "writeBatch")]
    pub async unsafe fn write_batch(
        &mut self,
        batch: &JsWriteBatch,
        await_durable: Option<bool>,
    ) -> Result<()> {
        let wb = {
            let mut guard = batch.inner.lock().await;
            guard
                .take()
                .ok_or_else(|| Error::from_reason("WriteBatch already consumed"))?
        };
        self.db_mut()?
            .write_with_options(wb, wo(await_durable))
            .await
            .map_err(to_napi_err)
    }

    // -----------------------------------------------------------------------
    // Transaction
    // -----------------------------------------------------------------------

    /// Begin an ACID transaction.
    #[napi]
    pub async fn begin(&self, isolation: Option<JsIsolationLevel>) -> Result<JsTransaction> {
        let level = match isolation.unwrap_or(JsIsolationLevel::Snapshot) {
            JsIsolationLevel::Snapshot => slatedb::IsolationLevel::Snapshot,
            JsIsolationLevel::SerializableSnapshot => slatedb::IsolationLevel::SerializableSnapshot,
        };
        let txn = self.db()?.begin(level).await.map_err(to_napi_err)?;
        Ok(JsTransaction {
            inner: Mutex::new(Some(txn)),
        })
    }

    // -----------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------

    /// Create a read-only point-in-time snapshot.
    #[napi]
    pub async fn snapshot(&self) -> Result<JsSnapshot> {
        let snap = self.db()?.snapshot().await.map_err(to_napi_err)?;
        Ok(JsSnapshot { inner: snap })
    }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    /// Create a checkpoint for backup/restore.
    #[napi(js_name = "createCheckpoint")]
    pub async fn create_checkpoint(
        &self,
        scope: Option<JsCheckpointScope>,
        lifetime_ms: Option<f64>,
        name: Option<String>,
    ) -> Result<JsCheckpointResult> {
        let s = match scope.unwrap_or(JsCheckpointScope::All) {
            JsCheckpointScope::All => CheckpointScope::All,
            JsCheckpointScope::Durable => CheckpointScope::Durable,
        };
        let opts = CheckpointOptions {
            lifetime: lifetime_ms.map(|ms| Duration::from_millis(ms as u64)),
            source: None,
            name,
        };
        let result = self
            .db()?
            .create_checkpoint(s, &opts)
            .await
            .map_err(to_napi_err)?;
        Ok(JsCheckpointResult {
            id: result.id.to_string(),
            manifest_id: result.manifest_id as i64,
        })
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    /// Get database metrics as a flat key-value object.
    #[napi]
    pub fn metrics(&self) -> Result<Vec<JsMetric>> {
        let registry = self.db()?.metrics();
        let names = registry.names();
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            if let Some(stat) = registry.lookup(name) {
                out.push(JsMetric {
                    name: name.to_string(),
                    value: stat.get(),
                });
            }
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------

    /// Close the database and free native resources.
    ///
    /// # Safety
    /// Called from JS via napi-rs. The `&mut self` borrow is safe because
    /// napi-rs prevents concurrent mutable access from JavaScript.
    #[napi]
    pub async unsafe fn close(&mut self) -> Result<()> {
        match &self.inner {
            DbKind::Writer(db) => db.close().await.map_err(to_napi_err),
            DbKind::Reader(r) => r.close().await.map_err(to_napi_err),
        }
    }
}

// ---------------------------------------------------------------------------
// JsMetric — returned from metrics()
// ---------------------------------------------------------------------------
#[napi(object)]
pub struct JsMetric {
    pub name: String,
    pub value: i64,
}

// ---------------------------------------------------------------------------
// WriteBatch class
// ---------------------------------------------------------------------------
#[napi(js_name = "WriteBatch")]
pub struct JsWriteBatch {
    inner: Mutex<Option<WriteBatch>>,
}

impl Default for JsWriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[napi]
impl JsWriteBatch {
    #[napi(constructor)]
    pub fn new() -> Self {
        JsWriteBatch {
            inner: Mutex::new(Some(WriteBatch::new())),
        }
    }

    /// Add a put to the batch (with optional TTL in ms).
    #[napi]
    pub async fn put(&self, key: Buffer, value: Buffer, ttl: Option<u32>) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let wb = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("WriteBatch already consumed"))?;
        wb.put_with_options(
            &key[..],
            &value[..],
            &build_put_options(ttl.map(|v| v as u64)),
        );
        Ok(())
    }

    /// Add a merge to the batch (with optional TTL in ms).
    #[napi]
    pub async fn merge(&self, key: Buffer, value: Buffer, ttl: Option<u32>) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let wb = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("WriteBatch already consumed"))?;
        let merge_opts = MergeOptions {
            ttl: match ttl.map(|v| v as u64) {
                None => Ttl::Default,
                Some(0) => Ttl::NoExpiry,
                Some(ms) => Ttl::ExpireAfter(ms),
            },
        };
        wb.merge_with_options(&key[..], &value[..], &merge_opts);
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
    /// Put within the transaction (with optional TTL).
    #[napi]
    pub async fn put(&self, key: Buffer, value: Buffer, ttl: Option<u32>) -> Result<()> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        txn.put_with_options(
            &key[..],
            &value[..],
            &build_put_options(ttl.map(|v| v as u64)),
        )
        .map_err(to_napi_err)
    }

    /// Get within the transaction.
    #[napi]
    pub async fn get(
        &self,
        key: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Option<Buffer>> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        let opts = build_read_options(read_level);
        match txn
            .get_with_options(&key[..], &opts)
            .await
            .map_err(to_napi_err)?
        {
            Some(val) => Ok(Some(Buffer::from(val.to_vec()))),
            None => Ok(None),
        }
    }

    /// Get within the transaction as UTF-8 string.
    #[napi(js_name = "getString")]
    pub async fn get_string(
        &self,
        key: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Option<String>> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        let opts = build_read_options(read_level);
        match txn
            .get_with_options(&key[..], &opts)
            .await
            .map_err(to_napi_err)?
        {
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

    /// Merge within the transaction.
    #[napi]
    pub async fn merge(&self, key: Buffer, value: Buffer, ttl: Option<u32>) -> Result<()> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        let merge_opts = MergeOptions {
            ttl: match ttl.map(|v| v as u64) {
                None => Ttl::Default,
                Some(0) => Ttl::NoExpiry,
                Some(ms) => Ttl::ExpireAfter(ms),
            },
        };
        txn.merge_with_options(&key[..], &value[..], &merge_opts)
            .map_err(to_napi_err)
    }

    /// Scan within the transaction [start, end).
    #[napi]
    pub async fn scan(
        &self,
        start: Option<Buffer>,
        end: Option<Buffer>,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Vec<KeyValue>> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        let opts = build_scan_options(read_level, None, None);
        let range = make_range(&start, &end);
        let iter = txn
            .scan_with_options(range, &opts)
            .await
            .map_err(to_napi_err)?;
        collect_iter(iter).await
    }

    /// Prefix scan within the transaction.
    #[napi(js_name = "scanPrefix")]
    pub async fn scan_prefix(
        &self,
        prefix: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Vec<KeyValue>> {
        let guard = self.inner.lock().await;
        let txn = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        let opts = build_scan_options(read_level, None, None);
        let iter = txn
            .scan_prefix_with_options(&prefix[..], &opts)
            .await
            .map_err(to_napi_err)?;
        collect_iter(iter).await
    }

    /// Commit the transaction.
    #[napi]
    pub async fn commit(&self, await_durable: Option<bool>) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let txn = guard
            .take()
            .ok_or_else(|| Error::from_reason("Transaction already consumed"))?;
        txn.commit_with_options(wo(await_durable))
            .await
            .map_err(to_napi_err)?;
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

// ---------------------------------------------------------------------------
// Snapshot class — read-only point-in-time view
// ---------------------------------------------------------------------------
#[napi(js_name = "Snapshot")]
pub struct JsSnapshot {
    inner: Arc<slatedb::DbSnapshot>,
}

#[napi]
impl JsSnapshot {
    /// Get raw bytes by key.
    #[napi]
    pub async fn get(
        &self,
        key: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Option<Buffer>> {
        let opts = build_read_options(read_level);
        match self
            .inner
            .get_with_options(&key[..], &opts)
            .await
            .map_err(to_napi_err)?
        {
            Some(val) => Ok(Some(Buffer::from(val.to_vec()))),
            None => Ok(None),
        }
    }

    /// Get value as UTF-8 string.
    #[napi(js_name = "getString")]
    pub async fn get_string(
        &self,
        key: Buffer,
        read_level: Option<JsDurabilityLevel>,
    ) -> Result<Option<String>> {
        let opts = build_read_options(read_level);
        match self
            .inner
            .get_with_options(&key[..], &opts)
            .await
            .map_err(to_napi_err)?
        {
            Some(val) => {
                let s = String::from_utf8(val.to_vec())
                    .map_err(|e| Error::from_reason(format!("invalid UTF-8: {e}")))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Range scan [start, end).
    #[napi]
    pub async fn scan(
        &self,
        start: Option<Buffer>,
        end: Option<Buffer>,
        read_level: Option<JsDurabilityLevel>,
        read_ahead_bytes: Option<u32>,
        max_fetch_tasks: Option<u32>,
    ) -> Result<Vec<KeyValue>> {
        let opts = build_scan_options(read_level, read_ahead_bytes, max_fetch_tasks);
        let range = make_range(&start, &end);
        let iter = self
            .inner
            .scan_with_options(range, &opts)
            .await
            .map_err(to_napi_err)?;
        collect_iter(iter).await
    }

    /// Prefix scan.
    #[napi(js_name = "scanPrefix")]
    pub async fn scan_prefix(
        &self,
        prefix: Buffer,
        read_level: Option<JsDurabilityLevel>,
        read_ahead_bytes: Option<u32>,
        max_fetch_tasks: Option<u32>,
    ) -> Result<Vec<KeyValue>> {
        let opts = build_scan_options(read_level, read_ahead_bytes, max_fetch_tasks);
        let iter = self
            .inner
            .scan_prefix_with_options(&prefix[..], &opts)
            .await
            .map_err(to_napi_err)?;
        collect_iter(iter).await
    }
}
