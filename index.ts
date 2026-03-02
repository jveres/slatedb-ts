import { createRequire } from "module";
import { join } from "path";
import { existsSync } from "fs";

// ---------------------------------------------------------------------------
// Load native module
// ---------------------------------------------------------------------------
const require = createRequire(import.meta.url);
const dir = new URL(".", import.meta.url).pathname;

const candidates = [
  `slatedb_napi.darwin-arm64.node`,
  `slatedb_napi.darwin-x64.node`,
  `slatedb_napi.linux-x64-gnu.node`,
  `slatedb_napi.linux-arm64-gnu.node`,
  `slatedb_napi.win32-x64-msvc.node`,
  `slatedb_napi.node`,
];

let nativePath: string | undefined;
for (const name of candidates) {
  const p = join(dir, name);
  if (existsSync(p)) {
    nativePath = p;
    break;
  }
}
if (!nativePath) {
  throw new Error(
    `slatedb: could not find native module. Tried: ${candidates.join(", ")}`,
  );
}

const native = require(nativePath);

// ---------------------------------------------------------------------------
// Re-export
// ---------------------------------------------------------------------------

/** SlateDB — async cloud-native embedded storage engine. */
export const SlateDB = native.SlateDb;

/** WriteBatch — batch multiple writes into a single atomic operation. */
export const WriteBatch = native.WriteBatch;

/** Transaction — ACID transaction with conflict detection. */
export const Transaction = native.Transaction;

/** Snapshot — read-only point-in-time view of the database. */
export const Snapshot = native.Snapshot;

/** Isolation levels for transactions. */
export const IsolationLevel = native.JsIsolationLevel;

/** Durability levels for reads. */
export const DurabilityLevel = native.JsDurabilityLevel;

/** Flush types. */
export const FlushType = native.JsFlushType;

/** Checkpoint scopes. */
export const CheckpointScope = native.JsCheckpointScope;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type KeyValue = { key: Buffer; value: Buffer };

export type Settings = {
  flushIntervalMs?: number;
  l0SstSizeBytes?: number;
  l0MaxSsts?: number;
  maxUnflushedBytes?: number;
  defaultTtlMs?: number;
  mergeOperator?: "string_concat" | "uint64_add";
  readOnly?: boolean;
};

export type CheckpointResult = {
  id: string;
  manifestId: number;
};

export type Metric = {
  name: string;
  value: number;
};

export default SlateDB;
