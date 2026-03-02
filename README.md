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
import {
  SlateDB, WriteBatch, IsolationLevel,
  DurabilityLevel, FlushType, CheckpointScope,
} from "./index";

// Open — in-memory, local filesystem, or cloud
const db = await SlateDB.open("/my-db", ":memory:");
// const db = await SlateDB.open("/my-db", "file:///tmp/slate");
// const db = await SlateDB.open("/my-db", "s3://my-bucket");

// Open with custom settings
const db2 = await SlateDB.open("/my-db", ":memory:", {
  flushIntervalMs: 100,
  l0SstSizeBytes: 64 * 1024 * 1024,
  mergeOperator: "uint64_add",
});

// Put / Get
await db.put(Buffer.from("hello"), Buffer.from("world"));
const val = await db.getString(Buffer.from("hello")); // "world"

// Binary keys & values
await db.put(Buffer.from([1, 2]), Buffer.from([3, 4]));
const got = await db.get(Buffer.from([1, 2])); // Buffer [3, 4]

// Fire-and-forget writes (skip durability wait)
await db.put(Buffer.from("fast"), Buffer.from("write"), false);

// Put with TTL (milliseconds)
await db.put(Buffer.from("temp"), Buffer.from("expires"), true, 60000);

// Range scan [start, end)
const items = await db.scan(Buffer.from("a"), Buffer.from("z"));

// Full scan
const all = await db.scan();

// Prefix scan
const users = await db.scanPrefix(Buffer.from("user:"));

// Write batch — atomic multi-put
const batch = new WriteBatch();
await batch.put(Buffer.from("k1"), Buffer.from("v1"));
await batch.put(Buffer.from("k2"), Buffer.from("v2"));
await batch.delete(Buffer.from("old"));
await db.writeBatch(batch);

// Merge — requires mergeOperator in settings
await db2.merge(Buffer.from("counter"), Buffer.from([5, 0, 0, 0, 0, 0, 0, 0]));

// Transaction — ACID with conflict detection
const txn = await db.begin(IsolationLevel.Snapshot);
await txn.put(Buffer.from("account_a"), Buffer.from("900"));
await txn.put(Buffer.from("account_b"), Buffer.from("1100"));
const balance = await txn.getString(Buffer.from("account_a")); // read-your-writes
const txnItems = await txn.scan(Buffer.from("account_"), Buffer.from("account_~"));
await txn.commit();

// Snapshot — read-only point-in-time view
const snap = await db.snapshot();
const snapVal = await snap.get(Buffer.from("hello"));
const snapItems = await snap.scanPrefix(Buffer.from("user:"));

// Checkpoint — backup/restore
const cp = await db.createCheckpoint(CheckpointScope.All, 3600000, "daily-backup");
console.log(cp.id, cp.manifestId);

// Metrics
const metrics = db.metrics(); // [{ name: "...", value: 42 }, ...]

// Flush, close
await db.flush(FlushType.Wal);
await db.close();
await db2.close();
```

## API

All operations are **async** and return Promises. Backpressure from cloud backends is handled by the Tokio runtime on background threads.

---

### Opening

#### `await SlateDB.open(path, url?, settings?)`

Open a database. `path` is the logical key prefix inside the store. `url` selects the backend (defaults to `":memory:"`). Pass `settings` to configure flush intervals, SST sizes, TTL, merge operators, etc.

| URL scheme       | Backend                   | Required env vars                                                              |
| ---------------- | ------------------------- | ------------------------------------------------------------------------------ |
| `:memory:`       | In-memory                 | —                                                                              |
| `file:///path`   | Local filesystem          | —                                                                              |
| `s3://bucket`    | AWS S3                    | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`                     |
| `s3://bucket`    | AWS S3 Express One Zone   | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_S3_EXPRESS=true` |
| `s3://bucket`    | Cloudflare R2             | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_REGION=auto` |
| `s3://bucket`    | MinIO                     | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_ALLOW_HTTP` |
| `az://container` | Azure Blob Storage        | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`                      |
| `gs://bucket`    | Google Cloud Storage      | `GOOGLE_SERVICE_ACCOUNT`                                                       |

For S3-compatible backends (AWS, R2, MinIO), credentials are loaded via [`AmazonS3Builder::from_env()`](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env) with `S3ConditionalPut::ETagMatch` (required for SlateDB's manifest fencing). `open()` has a 30-second timeout to prevent hanging on misconfigured backends.

**S3 Express One Zone** (directory buckets) are supported via `AWS_S3_EXPRESS=true`. The bucket name must follow the directory bucket naming convention (`bucket--azid--x-s3`). The `object_store` crate automatically constructs the zonal endpoint and uses `CreateSession` for short-lived session tokens. S3 Express supports `If-Match` conditional puts required for SlateDB's manifest fencing.

**Cloudflare R2 example:**

```bash
export AWS_ACCESS_KEY_ID="<r2-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<r2-secret-access-key>"
export AWS_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
export AWS_REGION="auto"

SLATEDB_TEST_URLS="s3://my-r2-bucket" bun test
```

**S3 Express One Zone example:**

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-east-1"
export AWS_S3_EXPRESS=true

bun run bencher -- db --url "s3://my-bucket--use1-az4--x-s3" --duration 30
```

**Settings:**

```typescript
type Settings = {
  flushIntervalMs?: number;    // WAL flush interval (null = disable auto-flush)
  l0SstSizeBytes?: number;     // Min memtable size before flush to L0
  l0MaxSsts?: number;          // Max L0 SSTs before backpressure
  maxUnflushedBytes?: number;  // Max unflushed bytes before writer backpressure
  defaultTtlMs?: number;       // Default TTL for put operations (null = no expiry)
  mergeOperator?: string;      // "string_concat" or "uint64_add" (null = no merge)
};
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

---

### Key-Value Operations

#### `await db.put(key, value, awaitDurable?, ttl?)`

Insert or update. Keys and values are `Buffer`.

- `awaitDurable` (default `true`) — wait for persistence to object storage. Pass `false` for fire-and-forget.
- `ttl` (optional, milliseconds) — per-key time-to-live. Pass `0` for no expiry. Omit to use the default TTL from settings.

#### `await db.get(key, durabilityLevel?)` → `Buffer | null`

Get raw bytes. Returns `null` if not found.

- `durabilityLevel` (optional) — `DurabilityLevel.Memory` (default, includes unflushed data) or `DurabilityLevel.Remote` (only durably stored data).

#### `await db.getString(key, durabilityLevel?)` → `string | null`

Convenience — decodes the value as UTF-8.

#### `await db.delete(key, awaitDurable?)`

Delete a key. `awaitDurable` defaults to `true`.

#### `await db.merge(key, value, awaitDurable?, ttl?)`

Merge a value using the configured merge operator. Requires `mergeOperator` to be set via `settings` in `open()`. The merge operator combines the existing value with the new value at read time.

Built-in operators:
- **`"string_concat"`** — appends bytes (useful for strings)
- **`"uint64_add"`** — adds 8-byte LE unsigned integers (useful for counters)

---

### Scanning

#### `await db.scan(start?, end?, durabilityLevel?, readAheadBytes?, maxFetchTasks?)` → `KeyValue[]`

Range scan `[start, end)`. Omit both for a full scan. Returns `{ key: Buffer, value: Buffer }[]`.

Optional scan parameters:
- `durabilityLevel` — filter by durability level
- `readAheadBytes` — bytes to read ahead (rounded up to nearest block size)
- `maxFetchTasks` — max concurrent block fetch tasks

#### `await db.scanPrefix(prefix, durabilityLevel?, readAheadBytes?, maxFetchTasks?)` → `KeyValue[]`

Prefix scan — returns all keys starting with the given prefix, in sorted order.

---

### Write Batch

#### `new WriteBatch()`

Create a batch for atomic multi-put.

| Method | Description |
| --- | --- |
| `await batch.put(key, value, ttl?)` | Add a put (with optional TTL in ms) |
| `await batch.merge(key, value, ttl?)` | Add a merge (with optional TTL in ms) |
| `await batch.delete(key)` | Add a delete |
| `await batch.free()` | Discard without writing |

#### `await db.writeBatch(batch, awaitDurable?)`

Atomically apply a `WriteBatch`. The batch is consumed and cannot be reused.

---

### Transactions

#### `await db.begin(isolation?)`

Begin an ACID transaction. Returns a `Transaction` object.

- `IsolationLevel.Snapshot` (default) — write-write conflict detection
- `IsolationLevel.SerializableSnapshot` — read-write + write-write conflict detection

| Method | Description |
| --- | --- |
| `await txn.put(key, value, ttl?)` | Put within the transaction (with optional TTL) |
| `await txn.get(key, durabilityLevel?)` | Read within the transaction (sees own writes) |
| `await txn.getString(key, durabilityLevel?)` | Read as UTF-8 string |
| `await txn.delete(key)` | Delete within the transaction |
| `await txn.merge(key, value, ttl?)` | Merge within the transaction |
| `await txn.scan(start?, end?, durabilityLevel?)` | Range scan within the transaction |
| `await txn.scanPrefix(prefix, durabilityLevel?)` | Prefix scan within the transaction |
| `await txn.commit(awaitDurable?)` | Commit (throws on conflict) |
| `await txn.rollback()` | Abort the transaction |

---

### Snapshots

#### `await db.snapshot()`

Create a read-only point-in-time view. Subsequent writes to the database are not visible through the snapshot.

| Method | Description |
| --- | --- |
| `await snap.get(key, durabilityLevel?)` | Get from the snapshot |
| `await snap.getString(key, durabilityLevel?)` | Get as UTF-8 string |
| `await snap.scan(start?, end?, durabilityLevel?, readAheadBytes?, maxFetchTasks?)` | Range scan |
| `await snap.scanPrefix(prefix, durabilityLevel?, readAheadBytes?, maxFetchTasks?)` | Prefix scan |

---

### Checkpoints

#### `await db.createCheckpoint(scope?, lifetimeMs?, name?)`

Create a checkpoint for backup/restore. Returns `{ id: string, manifestId: number }`.

- `scope` — `CheckpointScope.All` (default, flushes all data first) or `CheckpointScope.Durable` (only already-durable data, faster)
- `lifetimeMs` — optional checkpoint lifetime in milliseconds (null = no expiry)
- `name` — optional human-readable name

---

### Metrics

#### `db.metrics()` → `Metric[]`

Get runtime statistics as an array of `{ name: string, value: number }`. Includes counters for memtable operations, SST writes, compaction, cache hits/misses, etc.

---

### Flush & Close

#### `await db.flush(flushType?)`

Force-flush to object storage.

- `FlushType.Wal` (default) — flush the write-ahead log
- `FlushType.MemTable` — flush the memtable directly to L0

#### `await db.close()`

Close the database and free native resources. Flushes pending data before closing.

---

### Exports

```typescript
export { SlateDB, WriteBatch, Transaction, Snapshot, IsolationLevel,
         DurabilityLevel, FlushType, CheckpointScope };
export default SlateDB;
export type { KeyValue, Settings, CheckpointResult, Metric };
```

## Compaction

Compaction is **fully automatic**. When `SlateDB.open()` is called, SlateDB spawns a background compactor on the Tokio runtime that polls every 5 seconds, runs size-tiered compaction, and merges L0 SSTs into sorted runs (up to 4 concurrent compactions, 256 MiB max SST size). No manual intervention is needed — it runs for the lifetime of the database and shuts down cleanly on `db.close()`.

## Test

Integration tests are organized in 13 groups — 49 tests total:

| Group                   | Tests | Description                                                    |
| ----------------------- | ----: | -------------------------------------------------------------- |
| **full_example.rs**     |     5 | Ported from SlateDB's [`examples/src/full_example.rs`](https://github.com/slatedb/slatedb/blob/main/examples/src/full_example.rs) |
| **write batch**         |     4 | Atomic multi-put, deletes, non-durable, free-without-write     |
| **transactions**        |     6 | Commit, rollback, read-your-writes, delete, isolation          |
| **bridge extras**       |     8 | Binary keys, empty values, overwrite, scan order, edge cases   |
| **scan_prefix**         |     3 | Prefix scan, no matches, different prefixes                    |
| **snapshot**            |     4 | Point-in-time reads, snapshot scan, scanPrefix, getString      |
| **merge — string_concat** |  3 | Concatenation, create on merge, multiple merges                |
| **merge — uint64_add**    |  2 | Counter addition, create from zero                             |
| **WriteBatch merge**    |     1 | Merge operations in batches                                    |
| **transaction scan**    |     3 | Transaction scan, scanPrefix, merge within transactions        |
| **flush options**       |     2 | FlushType.Wal and FlushType.MemTable                           |
| **metrics**             |     1 | Runtime stats shape validation                                 |
| **checkpoint**          |     2 | Create with defaults, create with name and lifetime            |
| **open with settings**  |     2 | Custom settings, unknown merge operator error                  |
| **getString**           |     3 | DB getString, missing key, transaction getString               |

```bash
# In-memory (default)
bun test

# Local filesystem
SLATEDB_TEST_URLS="file:///tmp/slate" bun test

# Multiple backends at once
SLATEDB_TEST_URLS=":memory:,file:///tmp/slate" bun test

# AWS S3
SLATEDB_TEST_URLS="s3://my-bucket" bun test

# Cloudflare R2
SLATEDB_TEST_URLS="s3://my-r2-bucket" bun test

# Azure Blob Storage
SLATEDB_TEST_URLS="az://my-container" bun test
```

Every backend listed in `SLATEDB_TEST_URLS` (comma-separated) gets the full 49-test suite. Each run uses unique timestamped paths to avoid collisions on persistent stores.

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

# tune WAL flush interval (default: ~100ms)
bun run bencher -- db --await-durable --flush-interval 10
```

The `--flush-interval` flag sets the WAL flush interval in milliseconds. With `--await-durable`, every write blocks until the next flush completes — so the flush interval directly controls durable write throughput. The default (~100ms) caps throughput at ~10 flush cycles/sec regardless of backend speed. Lowering it (e.g. `--flush-interval 10`) lets faster backends like S3 Express One Zone show their latency advantage over standard S3.

Shorthand scripts:

```bash
bun run bencher:db                                      # = bencher.ts db
bun run bencher:txn                                     # = bencher.ts transaction
```

> **Note:** The `compaction` subcommand is not ported — it requires `CompactionExecuteBench`, an internal Rust struct that directly manipulates SSTs for synthetic benchmarking.

## Architecture

```
┌─────────────┐    N-API       ┌──────────────────┐     ┌────────────────┐
│  TypeScript │ ──────────▶    │  slatedb_napi    │ ──▶ │    SlateDB     │
│  (index.ts) │  JS classes    │  (napi-rs cdylib)│     │  (Rust crate)  │
└─────────────┘                └──────────────────┘     └────────────────┘
  SlateDB                       #[napi] classes:           Db, DbTransaction,
  WriteBatch                     SlateDB                   WriteBatch,
  Transaction                    JsWriteBatch              DbSnapshot,
  Snapshot                       JsTransaction             IsolationLevel
  IsolationLevel                 JsSnapshot
  DurabilityLevel                                               │
  FlushType                      async fn → Promise        object_store
  CheckpointScope                Tokio runtime (napi-rs)         │
                                                    ┌──────────────────────┐
                                                    │ S3 / R2 / Azure /    │
                                                    │ GCS / FS / Memory    │
                                                    └──────────────────────┘
```

The Rust layer (`src/lib.rs`) exposes native JS classes via napi-rs `#[napi]` macros:

| Class             | Methods                                                                         | Purpose                    |
| ----------------- | ------------------------------------------------------------------------------- | -------------------------- |
| **SlateDB**       | `open`, `close`, `put`, `get`, `getString`, `delete`, `merge`, `flush`, `scan`, `scanPrefix`, `writeBatch`, `begin`, `snapshot`, `createCheckpoint`, `metrics` | Database lifecycle + all ops |
| **WriteBatch**    | `new`, `put`, `merge`, `delete`, `free`                                         | Atomic batch writes        |
| **Transaction**   | `put`, `get`, `getString`, `delete`, `merge`, `scan`, `scanPrefix`, `commit`, `rollback` | ACID transactions          |
| **Snapshot**      | `get`, `getString`, `scan`, `scanPrefix`                                        | Read-only point-in-time    |

All async Rust futures are automatically converted to JS Promises by napi-rs. The Tokio runtime is managed internally by napi-rs.

## Project structure

```
├── Cargo.toml         Rust crate — napi-rs + slatedb + object_store (aws, azure)
├── build.rs           napi-build setup
├── bunfig.toml        Bun config — 30s test timeout for cloud backends
├── src/lib.rs         napi-rs native classes (SlateDB, WriteBatch, Transaction, Snapshot)
├── index.ts           Native module loader + re-exports
├── test.spec.ts       Integration tests — 49 tests across 13 groups, multi-backend
├── bench.ts           Micro-benchmark — comparison against SlateDB's Criterion bench
├── bencher.ts         Sustained throughput — ported from slatedb-bencher (db + transaction)
├── package.json       Scripts: build, test, bench, bencher, bencher:db, bencher:txn
└── .gitignore
```

## License

Apache-2.0
