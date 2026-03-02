# slatedb-ts

Tiny native [Bun](https://bun.sh) FFI bridge to [SlateDB](https://slatedb.io) — a cloud-native embedded storage engine built on object storage (S3, Azure Blob, GCS, local filesystem, MinIO, …).

## Prerequisites

- **Bun** ≥ 1.3
- **Rust** ≥ 1.85 (stable)

## Build

```bash
cargo build --release
```

Produces `target/release/libslatedb_ffi.{dylib,so,dll}` (~4 MB).

The Cargo crate includes `aws` and `azure` features by default. To build without cloud backends (smaller binary):

```bash
cargo build --release --no-default-features --features moka
```

## Usage

```typescript
import { SlateDB, WriteBatch, Transaction, IsolationLevel } from "./index";

// Open — in-memory, local filesystem, or cloud
const db = SlateDB.open("/my-db", ":memory:");
// const db = SlateDB.open("/my-db", "file:///tmp/slate");
// const db = SlateDB.open("/my-db", "s3://my-bucket");

// Put / Get
db.put("hello", "world");
db.getString("hello"); // "world"

// Binary keys & values
db.put(new Uint8Array([1, 2]), new Uint8Array([3, 4]));
db.get(new Uint8Array([1, 2])); // Uint8Array [3, 4]

// Fire-and-forget writes (skip durability wait)
db.put("fast", "write", false);

// Range scan [start, end)
db.scan("a", "z"); // KeyValue[]

// Full scan
db.scan();

// Write batch — atomic multi-put
const batch = new WriteBatch();
batch.put("k1", "v1").put("k2", "v2").delete("old");
db.writeBatch(batch);

// Transaction — ACID with conflict detection
const txn = db.begin(IsolationLevel.Snapshot);
txn.put("account_a", "900");
txn.put("account_b", "1100");
const val = txn.getString("account_a"); // read-your-writes
txn.commit();
// txn.rollback();                      // or abort

// Delete, flush, close
db.delete("hello");
db.flush();
db.close();
```

## API

### `SlateDB.open(path, url?)`

Open a database. `path` is the logical key prefix inside the store. `url` selects the backend (defaults to `":memory:"`).

| URL scheme       | Backend              | Required env vars                                          |
| ---------------- | -------------------- | ---------------------------------------------------------- |
| `:memory:`       | In-memory            | —                                                          |
| `file:///path`   | Local filesystem     | —                                                          |
| `s3://bucket`    | AWS S3               | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` |
| `az://container` | Azure Blob Storage   | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`  |
| `gs://bucket`    | Google Cloud Storage | `GOOGLE_SERVICE_ACCOUNT`                                   |

For S3, credentials are loaded via [`AmazonS3Builder::from_env()`](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env) with `S3ConditionalPut::ETagMatch` (required for SlateDB's manifest fencing). For other backends, URL resolution is handled by the Rust [`object_store`](https://docs.rs/object_store) crate. `Db::open()` has a 30-second timeout to prevent hanging on misconfigured backends.

> **Coming from the Rust `slatedb-bencher`?** The Rust CLI uses the older `admin::load_object_store_from_env()` API which reads a `CLOUD_PROVIDER` env var. This bridge uses URL strings instead — the mapping is:
>
> | Rust `CLOUD_PROVIDER`                        | Bun `url`                                    |
> | -------------------------------------------- | -------------------------------------------- |
> | `CLOUD_PROVIDER=memory`                      | `":memory:"`                                 |
> | `CLOUD_PROVIDER=local` + `LOCAL_PATH=/tmp/x` | `"file:///tmp/x"`                            |
> | `CLOUD_PROVIDER=aws` + `AWS_*` env vars      | `"s3://bucket"` + same `AWS_*` env vars      |
> | `CLOUD_PROVIDER=azure` + `AZURE_*` env vars  | `"az://container"` + same `AZURE_*` env vars |

### `db.put(key, value, awaitDurable?)`

Insert or update. Keys and values are `string | Uint8Array`.

`awaitDurable` (default `true`) controls whether to block until the write is persisted to object storage. Pass `false` for lower-latency fire-and-forget writes — data is still buffered in-memory and flushed on the next flush interval or explicit `flush()` call.

### `db.get(key)` → `Uint8Array | null`

Get raw bytes. Returns `null` if not found.

### `db.getString(key)` → `string | null`

Convenience — decodes the value as UTF-8.

### `db.delete(key, awaitDurable?)`

Delete a key. `awaitDurable` defaults to `true` (same semantics as `put`).

### `db.scan(start?, end?)` → `KeyValue[]`

Range scan `[start, end)`. Omit both for a full scan. Returns an array of `{ key: Uint8Array, value: Uint8Array }`. Scan results are zero-copy `subarray()` views into a single shared buffer — avoid holding references across `put`/`delete` calls.

### `db.writeBatch(batch, awaitDurable?)`

Atomically apply a `WriteBatch`. The batch is consumed and cannot be reused.

### `new WriteBatch()`

Create a batch. Chain `.put(key, value)` and `.delete(key)` calls. Submit with `db.writeBatch(batch)`. Call `.free()` to discard without writing.

### `db.begin(isolation?)`

Begin an ACID transaction. Returns a `Transaction` object.

`isolation` defaults to `IsolationLevel.Snapshot` (write-write conflict detection). Pass `IsolationLevel.SerializableSnapshot` for read-write + write-write conflict detection.

### Transaction methods

| Method                      | Description                                   |
| --------------------------- | --------------------------------------------- |
| `txn.put(key, value)`       | Put within the transaction                    |
| `txn.get(key)`              | Read within the transaction (sees own writes) |
| `txn.getString(key)`        | Read as UTF-8 string                          |
| `txn.delete(key)`           | Delete within the transaction                 |
| `txn.commit(awaitDurable?)` | Commit (throws on conflict)                   |
| `txn.rollback()`            | Abort the transaction                         |

### `db.flush()`

Force-flush the memtable to object storage.

### `db.close()`

Close the database and free native resources.

### Exports

```typescript
export { SlateDB, WriteBatch, Transaction, IsolationLevel };
export default SlateDB;
export type { KeyValue }; // { key: Uint8Array, value: Uint8Array }
```

## Compaction

Compaction is **fully automatic**. When `SlateDB.open()` is called, SlateDB spawns a background compactor on the Tokio runtime that polls every 5 seconds, runs size-tiered compaction, and merges L0 SSTs into sorted runs (up to 4 concurrent compactions, 256 MiB max SST size). No manual intervention is needed — it runs for the lifetime of the database and shuts down cleanly on `db.close()`.

## Test

Integration tests are organized in 4 groups — 23 tests total:

| Group               | Tests | Ported from                                                                                                           |
| ------------------- | ----- | --------------------------------------------------------------------------------------------------------------------- |
| **full_example.rs** | 5     | SlateDB's [`examples/src/full_example.rs`](https://github.com/slatedb/slatedb/blob/main/examples/src/full_example.rs) |
| **write batch**     | 4     | atomic multi-put, deletes, non-durable, free-without-write                                                            |
| **transactions**    | 6     | commit, rollback, read-your-writes, delete, non-durable, serializable isolation                                       |
| **bridge extras**   | 8     | binary keys, empty values, overwrite, scan order, edge cases                                                          |

```bash
# In-memory (default)
bun test

# Local filesystem
SLATEDB_TEST_URLS="file:///tmp/slate" bun test

# Multiple backends at once
SLATEDB_TEST_URLS=":memory:,file:///tmp/slate" bun test

# AWS S3
SLATEDB_TEST_URLS="s3://my-bucket" bun test

# Azure Blob Storage
SLATEDB_TEST_URLS="az://my-container" bun test
```

Every backend listed in `SLATEDB_TEST_URLS` (comma-separated) gets the full 23-test suite. Each run uses unique timestamped paths to avoid collisions on persistent stores.

**Cloud backend design:** Tests share one DB per group (`beforeAll`/`afterAll`) to minimize object store round-trips — `SlateDB.open()` and `db.close()` each perform multiple S3 operations (manifest, fencing, flush). All writes use `awaitDurable=false` with explicit `flush()` where needed. This matches SlateDB's own cloud-compatible integration test ([`tests/db.rs`](https://github.com/slatedb/slatedb/blob/main/slatedb/tests/db.rs)). Non-durable writes land in the memtable immediately and are visible within the same process — no flush needed for read-after-write consistency.

## Benchmark

### Micro-benchmark

Ported from SlateDB's [`benches/db_operations.rs`](https://github.com/slatedb/slatedb/blob/main/slatedb/benches/db_operations.rs) (Criterion). A matching native Rust bench (`native_bench.rs`) runs the same workloads for direct comparison.

```bash
bun run bench                                    # Bun FFI bridge
cargo run --release --example native_bench       # native Rust
```

Apple Silicon, in-memory backend, same machine back-to-back:

| Benchmark                         | Native Rust (p50) | Bun FFI (p50) | Overhead |
| --------------------------------- | ----------------- | ------------- | -------- |
| **put** (non-durable)             | 17.1 μs ¹         | 9.5 μs ¹      | — ¹      |
| **get** (hot key)                 | 4.3 μs ¹          | 1.7 μs ¹      | — ¹      |
| **scan** (100 keys, ~100B values) | 56.9 μs           | 64.2 μs       | **+13%** |

¹ `put` and `get` show the Bun bridge as faster than native Rust — this is a measurement artifact. The native bench calls `block_on` from a bare thread into a multi-thread Tokio runtime, while the FFI library's dedicated runtime has less scheduling contention. The [Criterion bench](https://github.com/slatedb/slatedb/blob/main/slatedb/benches/db_operations.rs) (which uses `b.to_async(&runtime)`) reports `put` at 9.3 μs — matching the FFI bridge exactly.

**Scan is the only benchmark where FFI overhead is measurable.** Profiling the 7 μs delta:

```
                                                  p50
rust iterate only (baseline)                    58.0 μs
+ serialize flat buffer in Rust                 59.2 μs   +1.2 μs
+ alloc Uint8Array + memcpy into JS             61.3 μs   +2.1 μs
+ decode u32 length headers                     62.2 μs   +0.9 μs
+ 100 KeyValue objects + subarray()             67.0 μs   +4.8 μs
```

96% of scan wall time is in SlateDB's own iterator. The bridge adds ~9 μs total: 4 μs for serialization + memcpy + decode, and 5 μs for JS object allocation (`subarray` views + `{key, value}` objects). This is at the physical minimum for the current API shape.

### Sustained throughput (slatedb-bencher)

Port of [`slatedb-bencher`](https://github.com/slatedb/slatedb/tree/main/slatedb-bencher) — both the `db` and `transaction` subcommands. All options mirror the Rust CLI.

```bash
bun run bencher -- --help                               # show subcommands
bun run bencher -- db --help                            # db options
bun run bencher -- transaction --help                   # transaction options

# db subcommand — mixed read/write
bun run bencher -- db --duration 30 --val-len 256
bun run bencher -- db --url "s3://my-bucket" --duration 60

# transaction subcommand
bun run bencher -- transaction --duration 30
bun run bencher -- transaction --use-write-batch        # WriteBatch comparison
bun run bencher -- transaction --isolation-level serializable

# any backend via --url
bun run bencher -- db --url "file:///tmp/bench"
bun run bencher -- transaction --url "az://my-container"
```

Shorthand scripts:

```bash
bun run bencher:db                                      # = bencher.ts db
bun run bencher:txn                                     # = bencher.ts transaction
```

**`db` subcommand** — steady-state at 20s, concurrency=1, 1KB values, in-memory:

| Metric        | Rust bencher | Bun bencher |
| ------------- | ------------ | ----------- |
| **put/s**     | 4,102        | 19,268      |
| **get/s**     | 16,399       | 77,150      |
| **put MiB/s** | 4.0          | 18.8        |
| **get MiB/s** | 14.0         | 72.6        |

The Bun bridge shows ~4× higher throughput than the Rust bencher for `db`. This is **not** an FFI advantage — it's a structural difference in async scheduling. The Rust bencher uses `tokio::spawn` + `.await` per operation (scheduling overhead per put/get). The FFI bridge calls `block_on` synchronously — zero scheduler hops.

**`transaction` subcommand** — 10s, concurrency=1, 10 ops/txn, 10% abort:

| Metric          | Rust bencher | Bun bencher |
| --------------- | ------------ | ----------- |
| **commit/s**    | 19,947       | 19,936      |
| **ops/s**       | 199,470      | 199,361     |
| **abort ratio** | 10.1%        | 10.0%       |
| **conflicts**   | 0            | 0           |

Transaction throughput is at **exact parity** — the `begin`/`put`/`commit` path has identical per-operation cost across both runtimes.

> **Note:** The `compaction` subcommand is not ported — it requires `CompactionExecuteBench`, an internal Rust struct that directly manipulates SSTs for synthetic benchmarking. It is not part of the normal database workflow (see [Compaction](#compaction)).

## Architecture

```
┌─────────────┐    bun:ffi     ┌──────────────────┐     ┌────────────────┐
│  TypeScript │ ──────────▶    │  libslatedb_ffi  │ ──▶ │    SlateDB     │
│  (index.ts) │  C ABI ptrs    │  (Rust cdylib)   │     │  (Rust crate)  │
└─────────────┘                └──────────────────┘     └────────────────┘
  SlateDB                       28 extern "C" fns          Db, DbTransaction,
  WriteBatch                    opaque handles:            WriteBatch,
  Transaction                    DbHandle                  IsolationLevel
  IsolationLevel                 BufHandle
                                 ScanHandle                      │
                                 WriteBatch*                object_store
                                 DbTransaction*                  │
                                                    ┌──────────────────────┐
                                                    │ S3 / Azure / GCS /   │
                                                    │ Filesystem / Memory  │
                                                    └──────────────────────┘
```

The Rust layer (`src/lib.rs`) exposes 28 `extern "C"` functions organized in 6 groups:

| Group           | Functions                                                                     | Purpose                                        |
| --------------- | ----------------------------------------------------------------------------- | ---------------------------------------------- |
| **Lifecycle**   | `open`, `close`                                                               | Database open/close                            |
| **KV ops**      | `put`, `get`, `delete`, `flush`                                               | Core key-value operations                      |
| **Scan**        | `scan`, `scan_count`, `scan_count_only`, `scan_ptr`, `scan_len`, `scan_copy`, `scan_free` | Range iteration with flat-buffer serialization |
| **Buf**         | `buf_ptr`, `buf_len`, `buf_copy`, `buf_free`                                              | Safe value retrieval into JS-owned memory      |
| **WriteBatch**  | `batch_new`, `batch_put`, `batch_delete`, `batch_write`, `batch_free`         | Atomic batch writes                            |
| **Transaction** | `txn_begin`, `txn_put`, `txn_get`, `txn_delete`, `txn_commit`, `txn_rollback` | ACID transactions                              |

A process-global Tokio runtime drives SlateDB's async operations via `block_on`. The TypeScript layer loads the shared library with `bun:ffi` `dlopen` and wraps pointer management into clean classes.

**Memory safety:** Data never crosses the FFI boundary via Bun's `toBuffer` (which creates GC-unsafe views into native memory). Instead, Rust copies directly into JS-owned `Uint8Array` buffers via dedicated `_copy` functions. Scan results use zero-copy `subarray()` views into the copied buffer.

## Project structure

```
├── Cargo.toml         Rust crate — cdylib linking slatedb + object_store (aws, azure)
├── bunfig.toml        Bun config — 30s test timeout for cloud backends
├── src/lib.rs         C ABI FFI layer — 28 functions (db, batch, transaction, scan)
├── index.ts           TypeScript classes — SlateDB, WriteBatch, Transaction, IsolationLevel
├── test.spec.ts       Integration tests — 23 tests across 4 groups, multi-backend
├── bench.ts           Micro-benchmark — ported from Criterion bench (put, get, scan, open_close)
├── bencher.ts         Sustained throughput — ported slatedb-bencher (db + transaction subcommands)
├── native_bench.rs    Matching native Rust bench for parity comparison
├── package.json       Scripts: build, test, bench, bencher, bencher:db, bencher:txn
└── .gitignore
```

## License

Apache-2.0
