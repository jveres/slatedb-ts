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
    `slatedb: could not find native module. Tried: ${candidates.join(", ")}`
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

/** Isolation levels for transactions. */
export const IsolationLevel = native.JsIsolationLevel;

export type KeyValue = { key: Buffer; value: Buffer };

export default SlateDB;
