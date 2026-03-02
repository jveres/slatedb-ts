# slatedb-ts

Async native [napi-rs](https://napi.rs) bridge to [SlateDB](https://slatedb.io) — a cloud-native embedded storage engine built on object storage (S3, Azure Blob, GCS, R2, MinIO, local filesystem, …).

Works on **Node.js** and **Bun** (N-API compatible).

## Prerequisites

- **Node.js** ≥ 18 or **Bun** ≥ 1.3
- **Rust** ≥ 1.85 (stable)

## Build

```bash
cargo build --release
cp target/release/libslatedb_napi.dylib slatedb_napi.darwin-arm64.node  # macOS arm64
# cp target/release/libslatedb_napi.so slatedb_napi.linux-x64-gnu.node  # Linux x64
```

Or use the shorthand:

```bash
bun run build   # or: npm run build
```

The Cargo crate includes `aws` and `azure` features by default. To build without cloud backends (smaller binary):

```bash
cargo build --release --no-default-features --features moka
```

## Usage

```typescript
import { SlateDB, WriteBatch, Transaction, IsolationLevel } from "./index";

// Open — in-memory, local filesystem, or cloud
const db = await SlateDB.open("/my-db", ":memory:");
// const db = await SlateDB.open("/my-db", "file:///tmp/slate");
// const db = await SlateDB.open("/my-db", "s3://my-bucket");

// Put / Get — all async, never blocks the main thread
await db.put(Buffer.from("hello"), Buffer.from("world"));
const val = await db.getString(Buffer.from("hello")); // "world"

// Binary keys & values
await db.put(Buffer.from([1, 2]), Buffer.from([3, 4]));
const got = await db.get(Buffer.from([1, 2])); // Buffer [3, 4]

// Fire-and-forget writes (skip durability wait)
await db.put(Buffer.from("fast"), Buffer.from("write"), false);

// Range scan [start, end)
const items = await db.scan(Buffer.from("a"), Buffer.from("z")); // KeyValue[]

// Full scan
const all = await db.scan();

// Write batch — atomic multi-put
const batch = new WriteBatch();
await batch.put(Buffer.from("k1"), Buffer.from("v1"));
await batch.put(Buffer.from("k2"), Buffer.from("v2"));
await batch.delete(Buffer.from("old"));
await db.writeBatch(batch);

// Transaction — ACID with conflict detection
const txn = await db.begin(IsolationLevel.Snapshot);
await txn.put(Buffer.from("account_a"), Buffer.from("900"));
await txn.put(Buffer.from("account_b"), Buffer.from("1100"));
const balance = await txn.getString(Buffer.from("account_a")); // read-your-writes
await txn.commit();
// await txn.rollback();  // or abort

// Delete, flush, close
await db.delete(Buffer.from("hello"));
await db.flush();
await db.close();
```

## API

All operations are **async** and return Promises. The main thread is never blocked — backpressure from cloud backends (S3 flush) is handled by the Tokio runtime on background threads.

### `await SlateDB.open(path, url?)`

Open a database. `path` is the logical key prefix inside the store. `url` selects the backend (defaults to `":memory:"`).

| URL scheme       | Backend              | Required env vars                                                              |
| ---------------- | -------------------- | ------------------------------------------------------------------------------ |
| `:memory:`       | In-memory            | —                                                                              |
| `file:///path`   | Local filesystem     | —                                                                              |
| `s3://bucket`    | AWS S3               | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`                     |
| `s3://bucket`    | Cloudflare R2        | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_REGION=auto` |
| `s3://bucket`    | MinIO                | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_ALLOW_HTTP` |
| `az://container` | Azure Blob Storage   | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`                      |
| `gs://bucket`    | Google Cloud Storage | `GOOGLE_SERVICE_ACCOUNT`                                                       |

For S3-compatible backends (AWS, R2, MinIO), credentials are loaded via [`AmazonS3Builder::from_env()`](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env) with `S3ConditionalPut::ETagMatch` (required for SlateDB's manifest fencing). R2 and MinIO support ETag-based conditional puts. For other backends, URL resolution is handled by the Rust [`object_store`](https://docs.rs/object_store) crate. `open()` has a 30-second timeout to prevent hanging on misconfigured backends.

**Cloudflare R2 example:**

```bash
export AWS_ACCESS_KEY_ID="<r2-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<r2-secret-access-key>"
export AWS_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
export AWS_REGION="auto"

SLATEDB_TEST_URLS="s3://my-r2-bucket" bun test
```

> **Coming from the Rust `slatedb-bencher`?** The Rust CLI uses the older `admin::load_object_store_from_env()` API which reads a `CLOUD_PROVIDER` env var. This bridge uses URL strings instead — the mapping is:
>
> | Rust `CLOUD_PROVIDER`                        | Bridge `url`                                                 |
> | -------------------------------------------- | ------------------------------------------------------------ |
> | `CLOUD_PROVIDER=memory`                      | `":memory:"`                                                 |
> | `CLOUD_PROVIDER=local` + `LOCAL_PATH=/tmp/x` | `"file:///tmp/x"`                                            |
> | `CLOUD_PROVIDER=aws` + `AWS_*` env vars      | `"s3://bucket"` + same `AWS_*` env vars                      |
> | `CLOUD_PROVIDER=azure` + `AZURE_*` env vars  | `"az://container"` + same `AZURE_*` env vars                 |
> | _(not supported)_                             | `"s3://bucket"` + `AWS_ENDPOINT` for R2/MinIO/S3-compatible  |

### `await db.put(key, value, awaitDurable?)`

Insert or update. Keys and values are `Buffer`.

`awaitDurable` (default `true`) controls whether to wait for persistence to object storage. Pass `false` for lower-latency fire-and-forget writes — data is still buffered in-memory and flushed on the next flush interval or explicit `flush()` call.

### `await db.get(key)` → `Buffer | null`

Get raw bytes. Returns `null` if not found.

### `await db.getString(key)` → `string | null`

Convenience — decodes the value as UTF-8.

### `await db.delete(key, awaitDurable?)`

Delete a key. `awaitDurable` defaults to `true` (same semantics as `put`).

### `await db.scan(start?, end?)` → `KeyValue[]`

Range scan `[start, end)`. Omit both for a full scan. Returns an array of `{ key: Buffer, value: Buffer }`.

### `await db.writeBatch(batch, awaitDurable?)`

Atomically apply a `WriteBatch`. The batch is consumed and cannot be reused.

### `new WriteBatch()`

Create a batch. Chain `await batch.put(key, value)` and `await batch.delete(key)` calls. Submit with `await db.writeBatch(batch)`. Call `await batch.free()` to discard without writing.

### `await db.begin(isolation?)`

Begin an ACID transaction. Returns a `Transaction` object.

`isolation` defaults to `IsolationLevel.Snapshot` (write-write conflict detection). Pass `IsolationLevel.SerializableSnapshot` for read-write + write-write conflict detection.

### Transaction methods

| Method                            | Description                                   |
| --------------------------------- | --------------------------------------------- |
| `await txn.put(key, value)`       | Put within the transaction                    |
| `await txn.get(key)`              | Read within the transaction (sees own writes) |
| `await txn.getString(key)`        | Read as UTF-8 string                          |
| `await txn.delete(key)`           | Delete within the transaction                 |
| `await txn.commit(awaitDurable?)` | Commit (throws on conflict)                   |
| `await txn.rollback()`            | Abort the transaction                         |

### `await db.flush()`

Force-flush the memtable to object storage.

### `await db.close()`

Close the database and free native resources.

### Exports

```typescript
export { SlateDB, WriteBatch, Transaction, IsolationLevel };
export default SlateDB;
export type { KeyValue }; // { key: Buffer, value: Buffer }
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

# Cloudflare R2 (set AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION=auto)
SLATEDB_TEST_URLS="s3://my-r2-bucket" bun test

# Azure Blob Storage
SLATEDB_TEST_URLS="az://my-container" bun test
```

Every backend listed in `SLATEDB_TEST_URLS` (comma-separated) gets the full 23-test suite. Each run uses unique timestamped paths to avoid collisions on persistent stores.

**Cloud backend design:** Tests share one DB per group (`beforeAll`/`afterAll`) to minimize object store round-trips — `open()` and `close()` each perform multiple S3 operations (manifest, fencing, flush). All writes use `awaitDurable=false` with explicit `flush()` where needed. This matches SlateDB's own cloud-compatible integration test ([`tests/db.rs`](https://github.com/slatedb/slatedb/blob/main/slatedb/tests/db.rs)). Non-durable writes land in the memtable immediately and are visible within the same process — no flush needed for read-after-write consistency.

## Benchmark

### Micro-benchmark

Ported from SlateDB's [`benches/db_operations.rs`](https://github.com/slatedb/slatedb/blob/main/slatedb/benches/db_operations.rs) (Criterion).

```bash
bun run bench
```

Apple Silicon, in-memory backend, same machine:

| Benchmark                         | SlateDB Criterion | napi-rs (p50) | Overhead  |
| --------------------------------- | ----------------: | ------------: | --------: |
| **put** (non-durable)             | 9.42 μs           | 13.42 μs      | **+42%**  |
| **get** (hot key)                 | 1.16 μs           | 15.33 μs      | **+13×**  |
| **scan** (100 keys, ~100B values) | 53.37 μs          | 119.50 μs     | **+2.2×** |
| **open_close**                    | 172.41 μs         | 111.33 μs     | −35% ¹    |

¹ `open_close` appears faster in napi-rs because Criterion uses `b.to_async(&runtime)` which adds per-iteration scheduling overhead; the napi-rs Tokio runtime runs the future directly.

The **put** overhead (+42%) is the napi-rs bridge cost: Promise creation, N-API call dispatch, and Buffer marshalling. **get** shows the largest relative overhead (13×) because the native operation is extremely fast (~1μs memtable lookup) and each call must still pay the fixed ~14μs async bridge round-trip. **scan** at 2.2× reflects iterator materialization across the bridge — collecting all key-value pairs into a JS array of objects.

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
bun run bencher -- transaction --use-write-batch
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

> **Note:** The `compaction` subcommand is not ported — it requires `CompactionExecuteBench`, an internal Rust struct that directly manipulates SSTs for synthetic benchmarking. It is not part of the normal database workflow (see [Compaction](#compaction)).

## Architecture

```
┌─────────────┐    N-API       ┌──────────────────┐     ┌────────────────┐
│  TypeScript │ ──────────▶    │  slatedb_napi    │ ──▶ │    SlateDB     │
│  (index.ts) │  JS classes    │  (napi-rs cdylib)│     │  (Rust crate)  │
└─────────────┘                └──────────────────┘     └────────────────┘
  SlateDB                       #[napi] classes:           Db, DbTransaction,
  WriteBatch                     SlateDB                   WriteBatch,
  Transaction                    JsWriteBatch              IsolationLevel
  IsolationLevel                 JsTransaction
                                                                │
                                 async fn → Promise        object_store
                                 Tokio runtime (napi-rs)         │
                                                    ┌──────────────────────┐
                                                    │ S3 / R2 / Azure /    │
                                                    │ GCS / FS / Memory    │
                                                    └──────────────────────┘
```

The Rust layer (`src/lib.rs`) exposes native JS classes via napi-rs `#[napi]` macros:

| Class             | Methods                                            | Purpose                     |
| ----------------- | -------------------------------------------------- | --------------------------- |
| **SlateDB**       | `open`, `close`, `put`, `get`, `getString`, `delete`, `flush`, `scan`, `writeBatch`, `begin` | Database lifecycle + KV ops |
| **WriteBatch**    | `new`, `put`, `delete`, `free`                     | Atomic batch writes         |
| **Transaction**   | `put`, `get`, `getString`, `delete`, `commit`, `rollback` | ACID transactions           |

All async Rust futures are automatically converted to JS Promises by napi-rs. The Tokio runtime is managed internally — no manual `block_on` calls, no main-thread blocking. Backpressure from cloud backends (S3 flush waits) is handled entirely on background threads.

## Project structure

```
├── Cargo.toml         Rust crate — napi-rs + slatedb + object_store (aws, azure)
├── build.rs           napi-build setup
├── bunfig.toml        Bun config — 30s test timeout for cloud backends
├── src/lib.rs         napi-rs native classes — SlateDB, WriteBatch, Transaction
├── index.ts           Native module loader + re-exports
├── test.spec.ts       Integration tests — 23 tests across 4 groups, multi-backend
├── bench.ts           Micro-benchmark — comparison against SlateDB's Criterion bench
├── bencher.ts         Sustained throughput — ported from slatedb-bencher (db + transaction)
├── package.json       Scripts: build, test, bench, bencher, bencher:db, bencher:txn
└── .gitignore
```

## License

Apache-2.0
