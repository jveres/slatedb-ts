use object_store::memory::InMemory;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, DbTransaction, IsolationLevel, WriteBatch};
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Tokio runtime – one per process
// ---------------------------------------------------------------------------
static RT: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
});

// ---------------------------------------------------------------------------
// Opaque handles
// ---------------------------------------------------------------------------
pub struct DbHandle {
    db: Db,
}

/// Heap-allocated byte buffer returned to the caller.
pub struct BufHandle {
    data: Vec<u8>,
}

/// Heap-allocated scan result.
pub struct ScanHandle {
    /// Flat buffer: [key_len:u32_le, key, val_len:u32_le, val]*
    data: Vec<u8>,
    count: u32,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    unsafe { CStr::from_ptr(s).to_str().unwrap_or("") }
}

/// Resolve an object store from a URL string.
///
/// Supported schemes:
///   `:memory:`       — in-memory (default)
///   `file:///path`   — local filesystem
///   `s3://bucket`    — AWS S3, Cloudflare R2, MinIO (reads AWS_* env vars)
///   `az://container` — Azure Blob (reads AZURE_* env vars)
///
/// For S3-compatible backends we use `AmazonS3Builder::from_env()` with
/// `S3ConditionalPut::ETagMatch`, matching SlateDB's own bencher
/// (`admin::load_aws`). This reads AWS_ENDPOINT for R2/MinIO, AWS_REGION,
/// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY correctly from env vars.
/// The generic `Db::resolve_object_store` lowercases env vars for
/// `parse_url_opts` which can misconfigure the client.
fn resolve_store(url: *const c_char) -> Arc<dyn ObjectStore> {
    let u = cstr_to_str(url);
    if u.is_empty() || u == ":memory:" {
        Arc::new(InMemory::new())
    } else if u.starts_with("s3://") {
        use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
        let bucket = u.strip_prefix("s3://").unwrap().trim_end_matches('/');
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build S3 store");
        Arc::new(store)
    } else if u.starts_with("az://") || u.starts_with("azure://") {
        // Fall back to generic resolver for Azure/GCS
        Db::resolve_object_store(u).expect("failed to resolve object store from URL")
    } else if u.starts_with("file://") {
        let path = u.strip_prefix("file://").unwrap();
        let lfs = LocalFileSystem::new_with_prefix(path)
            .expect("failed to create local filesystem store");
        Arc::new(lfs)
    } else {
        Db::resolve_object_store(u).expect("failed to resolve object store from URL")
    }
}

// ===========================================================================
// Database lifecycle
// ===========================================================================

/// Open a database. Returns opaque handle (null on error).
/// Times out after 30 seconds to avoid hanging on misconfigured object stores.
#[no_mangle]
pub extern "C" fn slatedb_open(path: *const c_char, url: *const c_char) -> *mut DbHandle {
    let p = cstr_to_str(path);
    let store = resolve_store(url);
    match RT.block_on(async {
        tokio::time::timeout(Duration::from_secs(30), Db::open(p, store)).await
    }) {
        Ok(Ok(db)) => Box::into_raw(Box::new(DbHandle { db })),
        Ok(Err(e)) => {
            eprintln!("slatedb_open: {e}");
            ptr::null_mut()
        }
        Err(_) => {
            eprintln!("slatedb_open: timed out after 30s — check credentials, region, and bucket access");
            ptr::null_mut()
        }
    }
}

/// Close the database and free the handle.
#[no_mangle]
pub extern "C" fn slatedb_close(h: *mut DbHandle) {
    if h.is_null() { return; }
    let h = unsafe { Box::from_raw(h) };
    let _ = RT.block_on(h.db.close());
}

// ===========================================================================
// Key-value operations
// ===========================================================================

/// Put. `await_durable`: 1 = wait for persistence (default), 0 = return immediately.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn slatedb_put(
    h: *mut DbHandle,
    key: *const u8, key_len: usize,
    val: *const u8, val_len: usize,
    await_durable: i32,
) -> i32 {
    let db = &unsafe { &*h }.db;
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    let v = if val.is_null() || val_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(val, val_len) }
    };
    let result = if await_durable == 0 {
        RT.block_on(db.put_with_options(
            k,
            v,
            &PutOptions::default(),
            &WriteOptions { await_durable: false },
        ))
    } else {
        RT.block_on(db.put(k, v))
    };
    match result {
        Ok(_) => 0,
        Err(e) => { eprintln!("slatedb_put: {e}"); -1 }
    }
}

/// Get. Returns a BufHandle (null = not found / error).
/// Use `slatedb_buf_ptr`, `slatedb_buf_len`, `slatedb_buf_free`.
#[no_mangle]
pub extern "C" fn slatedb_get(
    h: *mut DbHandle,
    key: *const u8, key_len: usize,
) -> *mut BufHandle {
    let db = &unsafe { &*h }.db;
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    match RT.block_on(db.get(k)) {
        Ok(Some(val)) => Box::into_raw(Box::new(BufHandle { data: val.to_vec() })),
        Ok(None) => ptr::null_mut(),
        Err(e) => { eprintln!("slatedb_get: {e}"); ptr::null_mut() }
    }
}

/// Delete. `await_durable`: 1 = wait for persistence (default), 0 = return immediately.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn slatedb_delete(
    h: *mut DbHandle,
    key: *const u8, key_len: usize,
    await_durable: i32,
) -> i32 {
    let db = &unsafe { &*h }.db;
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    let result = if await_durable == 0 {
        RT.block_on(db.delete_with_options(k, &WriteOptions { await_durable: false }))
    } else {
        RT.block_on(db.delete(k))
    };
    match result {
        Ok(_) => 0,
        Err(e) => { eprintln!("slatedb_delete: {e}"); -1 }
    }
}

/// Flush. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn slatedb_flush(h: *mut DbHandle) -> i32 {
    let db = &unsafe { &*h }.db;
    match RT.block_on(db.flush()) {
        Ok(_) => 0,
        Err(e) => { eprintln!("slatedb_flush: {e}"); -1 }
    }
}

// ===========================================================================
// Scan
// ===========================================================================

/// Scan [start, end). NULL ptr = unbounded on that side.
/// Returns a ScanHandle (null on error / empty).
/// Use `slatedb_scan_count`, `slatedb_scan_ptr`, `slatedb_scan_len`,
/// `slatedb_scan_free`.
#[no_mangle]
pub extern "C" fn slatedb_scan(
    h: *mut DbHandle,
    start: *const u8, start_len: usize,
    end: *const u8, end_len: usize,
) -> *mut ScanHandle {
    let db = &unsafe { &*h }.db;

    let range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) = {
        let lo = if start.is_null() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Included(
                unsafe { std::slice::from_raw_parts(start, start_len) }.to_vec(),
            )
        };
        let hi = if end.is_null() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Excluded(
                unsafe { std::slice::from_raw_parts(end, end_len) }.to_vec(),
            )
        };
        (lo, hi)
    };

    RT.block_on(async {
        let mut iter = match db.scan(range).await {
            Ok(it) => it,
            Err(e) => { eprintln!("slatedb_scan: {e}"); return ptr::null_mut(); }
        };

        let mut buf: Vec<u8> = Vec::new();
        let mut count: u32 = 0;

        while let Ok(Some(kv)) = iter.next().await {
            buf.extend_from_slice(&(kv.key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&kv.key);
            buf.extend_from_slice(&(kv.value.len() as u32).to_le_bytes());
            buf.extend_from_slice(&kv.value);
            count += 1;
        }

        if count == 0 {
            return ptr::null_mut();
        }

        Box::into_raw(Box::new(ScanHandle { data: buf, count }))
    })
}

// ===========================================================================
// Buffer accessors
// ===========================================================================

#[no_mangle]
pub extern "C" fn slatedb_buf_ptr(b: *const BufHandle) -> *const u8 {
    if b.is_null() { return ptr::null(); }
    unsafe { &*b }.data.as_ptr()
}

#[no_mangle]
pub extern "C" fn slatedb_buf_len(b: *const BufHandle) -> usize {
    if b.is_null() { return 0; }
    unsafe { &*b }.data.len()
}

#[no_mangle]
pub extern "C" fn slatedb_buf_free(b: *mut BufHandle) {
    if !b.is_null() { unsafe { let _ = Box::from_raw(b); }; }
}

// ===========================================================================
// Scan result accessors
// ===========================================================================

#[no_mangle]
pub extern "C" fn slatedb_scan_count(s: *const ScanHandle) -> u32 {
    if s.is_null() { return 0; }
    unsafe { &*s }.count
}

#[no_mangle]
pub extern "C" fn slatedb_scan_ptr(s: *const ScanHandle) -> *const u8 {
    if s.is_null() { return ptr::null(); }
    unsafe { &*s }.data.as_ptr()
}

#[no_mangle]
pub extern "C" fn slatedb_scan_len(s: *const ScanHandle) -> usize {
    if s.is_null() { return 0; }
    unsafe { &*s }.data.len()
}

#[no_mangle]
pub extern "C" fn slatedb_scan_free(s: *mut ScanHandle) {
    if !s.is_null() { unsafe { let _ = Box::from_raw(s); }; }
}

// ===========================================================================
// Safe copy — write into a caller-owned buffer (avoids toBuffer on native mem)
// ===========================================================================

/// Copy BufHandle data into `dst`. Returns bytes written.
#[no_mangle]
pub extern "C" fn slatedb_buf_copy(b: *const BufHandle, dst: *mut u8, dst_len: usize) -> usize {
    if b.is_null() || dst.is_null() { return 0; }
    let data = &unsafe { &*b }.data;
    let n = data.len().min(dst_len);
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), dst, n); }
    n
}

/// Copy ScanHandle data into `dst`. Returns bytes written.
#[no_mangle]
pub extern "C" fn slatedb_scan_copy(s: *const ScanHandle, dst: *mut u8, dst_len: usize) -> usize {
    if s.is_null() || dst.is_null() { return 0; }
    let data = &unsafe { &*s }.data;
    let n = data.len().min(dst_len);
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), dst, n); }
    n
}

// ===========================================================================
// WriteBatch
// ===========================================================================

/// Create an empty WriteBatch.
#[no_mangle]
pub extern "C" fn slatedb_batch_new() -> *mut WriteBatch {
    Box::into_raw(Box::new(WriteBatch::new()))
}

/// Add a put to the batch.
#[no_mangle]
pub extern "C" fn slatedb_batch_put(
    b: *mut WriteBatch,
    key: *const u8, key_len: usize,
    val: *const u8, val_len: usize,
) {
    let batch = unsafe { &mut *b };
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    let v = if val.is_null() || val_len == 0 { &[] as &[u8] }
            else { unsafe { std::slice::from_raw_parts(val, val_len) } };
    batch.put(k, v);
}

/// Add a delete to the batch.
#[no_mangle]
pub extern "C" fn slatedb_batch_delete(
    b: *mut WriteBatch,
    key: *const u8, key_len: usize,
) {
    let batch = unsafe { &mut *b };
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    batch.delete(k);
}

/// Submit the batch. `await_durable`: 1 = wait, 0 = return immediately.
/// Consumes the batch (frees it). Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn slatedb_batch_write(
    h: *mut DbHandle,
    b: *mut WriteBatch,
    await_durable: i32,
) -> i32 {
    let db = &unsafe { &*h }.db;
    let batch = unsafe { *Box::from_raw(b) };
    let result = if await_durable == 0 {
        RT.block_on(db.write_with_options(batch, &WriteOptions { await_durable: false }))
    } else {
        RT.block_on(db.write(batch))
    };
    match result {
        Ok(_) => 0,
        Err(e) => { eprintln!("slatedb_batch_write: {e}"); -1 }
    }
}

/// Free a batch without writing it.
#[no_mangle]
pub extern "C" fn slatedb_batch_free(b: *mut WriteBatch) {
    if !b.is_null() { unsafe { let _ = Box::from_raw(b); }; }
}

// ===========================================================================
// Transactions
// ===========================================================================

/// Begin a transaction. `isolation`: 0 = Snapshot (default), 1 = SerializableSnapshot.
/// Returns an opaque handle (null on error).
#[no_mangle]
pub extern "C" fn slatedb_txn_begin(
    h: *mut DbHandle,
    isolation: i32,
) -> *mut DbTransaction {
    let db = &unsafe { &*h }.db;
    let level = if isolation == 1 {
        IsolationLevel::SerializableSnapshot
    } else {
        IsolationLevel::Snapshot
    };
    match RT.block_on(db.begin(level)) {
        Ok(txn) => Box::into_raw(Box::new(txn)),
        Err(e) => { eprintln!("slatedb_txn_begin: {e}"); ptr::null_mut() }
    }
}

/// Put within a transaction. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn slatedb_txn_put(
    t: *mut DbTransaction,
    key: *const u8, key_len: usize,
    val: *const u8, val_len: usize,
) -> i32 {
    let txn = unsafe { &*t };
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    let v = if val.is_null() || val_len == 0 { &[] as &[u8] }
            else { unsafe { std::slice::from_raw_parts(val, val_len) } };
    match txn.put(k, v) {
        Ok(_) => 0,
        Err(e) => { eprintln!("slatedb_txn_put: {e}"); -1 }
    }
}

/// Get within a transaction. Returns BufHandle (null = not found / error).
#[no_mangle]
pub extern "C" fn slatedb_txn_get(
    t: *mut DbTransaction,
    key: *const u8, key_len: usize,
) -> *mut BufHandle {
    let txn = unsafe { &*t };
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    match RT.block_on(txn.get(k)) {
        Ok(Some(val)) => Box::into_raw(Box::new(BufHandle { data: val.to_vec() })),
        Ok(None) => ptr::null_mut(),
        Err(e) => { eprintln!("slatedb_txn_get: {e}"); ptr::null_mut() }
    }
}

/// Delete within a transaction. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn slatedb_txn_delete(
    t: *mut DbTransaction,
    key: *const u8, key_len: usize,
) -> i32 {
    let txn = unsafe { &*t };
    let k = unsafe { std::slice::from_raw_parts(key, key_len) };
    match txn.delete(k) {
        Ok(_) => 0,
        Err(e) => { eprintln!("slatedb_txn_delete: {e}"); -1 }
    }
}

/// Commit the transaction. `await_durable`: 1 = wait, 0 = return immediately.
/// Consumes the handle. Returns 0 on success, -1 on error (conflict), -2 on closed.
#[no_mangle]
pub extern "C" fn slatedb_txn_commit(
    t: *mut DbTransaction,
    await_durable: i32,
) -> i32 {
    let txn = unsafe { *Box::from_raw(t) };
    let result = if await_durable == 0 {
        RT.block_on(txn.commit_with_options(&WriteOptions { await_durable: false }))
    } else {
        RT.block_on(txn.commit())
    };
    match result {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("slatedb_txn_commit: {e}");
            -1
        }
    }
}

/// Rollback (abort) the transaction. Consumes the handle.
#[no_mangle]
pub extern "C" fn slatedb_txn_rollback(t: *mut DbTransaction) {
    let txn = unsafe { *Box::from_raw(t) };
    txn.rollback();
}

/// Scan that only counts — no serialization. For profiling FFI overhead.
#[no_mangle]
pub extern "C" fn slatedb_scan_count_only(
    h: *mut DbHandle,
    start: *const u8, start_len: usize,
    end: *const u8, end_len: usize,
) -> u32 {
    let db = &unsafe { &*h }.db;
    let range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) = {
        let lo = if start.is_null() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Included(
                unsafe { std::slice::from_raw_parts(start, start_len) }.to_vec(),
            )
        };
        let hi = if end.is_null() {
            std::ops::Bound::Unbounded
        } else {
            std::ops::Bound::Excluded(
                unsafe { std::slice::from_raw_parts(end, end_len) }.to_vec(),
            )
        };
        (lo, hi)
    };
    RT.block_on(async {
        let mut iter = match db.scan(range).await {
            Ok(it) => it,
            Err(_) => return 0,
        };
        let mut count: u32 = 0;
        while let Ok(Some(_)) = iter.next().await {
            count += 1;
        }
        count
    })
}
