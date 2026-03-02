#!/usr/bin/env bun
/**
 * slatedb-bencher — port of slatedb-bencher's `db` and `transaction` subcommands.
 *
 * Usage:
 *   bun run bencher.ts db [options]
 *   bun run bencher.ts transaction [options]
 *   bun run bencher.ts --help
 *
 * @see https://github.com/slatedb/slatedb/tree/main/slatedb-bencher
 */
import { parseArgs } from "util";
import { SlateDB, WriteBatch, IsolationLevel, Transaction } from "./index";

// ---------------------------------------------------------------------------
// Stats recorder — port of stats.rs rolling windows
// ---------------------------------------------------------------------------
const WINDOW_SIZE_MS        = 10_000;
const MAX_WINDOWS           = 180;
const STAT_DUMP_INTERVAL_MS = 10_000;
const STAT_DUMP_LOOKBACK_MS = 60_000;
const REPORT_INTERVAL_MS    = 100;

interface Window {
  startMs: number;
  endMs: number;
  [key: string]: number;
}

class StatsRecorder {
  windows: Window[] = [];
  totals: Record<string, number> = {};

  record(nowMs: number, values: Record<string, number>) {
    for (const [k, v] of Object.entries(values)) {
      this.totals[k] = (this.totals[k] ?? 0) + v;
    }
    this.maybeRoll(nowMs);
    const w = this.windows[0];
    for (const [k, v] of Object.entries(values)) {
      w[k] = (w[k] ?? 0) + v;
    }
  }

  private maybeRoll(nowMs: number) {
    if (this.windows.length === 0) {
      this.windows.unshift({ startMs: nowMs, endMs: nowMs + WINDOW_SIZE_MS });
      return;
    }
    while (nowMs >= this.windows[0].endMs) {
      const end = this.windows[0].endMs;
      this.windows.unshift({ startMs: end, endMs: end + WINDOW_SIZE_MS });
      if (this.windows.length > MAX_WINDOWS) this.windows.pop();
    }
  }

  getRelevant(): { relevant: Window[]; rangeStart: number; rangeEnd: number; intervalS: number } | null {
    if (this.windows.length < 2) return null;
    const rangeEnd = this.windows[0].startMs;
    const cutoff = rangeEnd - STAT_DUMP_LOOKBACK_MS;
    const relevant = this.windows.slice(1).filter(w => w.startMs >= cutoff);
    if (relevant.length === 0) return null;
    const rangeStart = relevant[relevant.length - 1].startMs;
    const intervalS = (rangeEnd - rangeStart) / 1000;
    if (intervalS <= 0) return null;
    return { relevant, rangeStart, rangeEnd, intervalS };
  }

  sumField(relevant: Window[], field: string): number {
    return relevant.reduce((a, w) => a + (w[field] ?? 0), 0);
  }

  total(field: string): number {
    return this.totals[field] ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Key generators — port of db.rs RandomKeyGenerator / FixedSetKeyGenerator
// ---------------------------------------------------------------------------
function randomBytes(len: number): Uint8Array {
  const buf = new Uint8Array(len);
  crypto.getRandomValues(buf);
  return buf;
}

interface KeyGenerator {
  nextKey(): Uint8Array;
  usedKey(): Uint8Array;
}

class RandomKeyGenerator implements KeyGenerator {
  private usedKeys: Uint8Array[] = [];
  constructor(private keyLen: number) {}

  nextKey(): Uint8Array {
    const key = randomBytes(this.keyLen);
    if (this.usedKeys.length < 10_000) {
      this.usedKeys.push(key);
    } else {
      this.usedKeys[Math.floor(Math.random() * this.usedKeys.length)] = key;
    }
    return key;
  }

  usedKey(): Uint8Array {
    if (this.usedKeys.length === 0) return this.nextKey();
    return this.usedKeys[Math.floor(Math.random() * this.usedKeys.length)];
  }
}

class FixedSetKeyGenerator implements KeyGenerator {
  private keys: Uint8Array[];
  private usedKeys: Uint8Array[] = [];

  constructor(keyLen: number, keyCount: number) {
    const gen = new RandomKeyGenerator(keyLen);
    this.keys = Array.from({ length: keyCount }, () => gen.nextKey());
  }

  nextKey(): Uint8Array {
    const key = this.keys[Math.floor(Math.random() * this.keys.length)];
    this.usedKeys.push(key);
    return key;
  }

  usedKey(): Uint8Array {
    if (this.usedKeys.length === 0) return this.nextKey();
    return this.usedKeys[Math.floor(Math.random() * this.usedKeys.length)];
  }
}

function makeKeyGen(type: string, keyLen: number, keyCount: number): KeyGenerator {
  return type === "random"
    ? new RandomKeyGenerator(keyLen)
    : new FixedSetKeyGenerator(keyLen, keyCount);
}

// ---------------------------------------------------------------------------
// Cloud warning
// ---------------------------------------------------------------------------
function cloudWarning(url: string) {
  if (/^(s3|az|gs):\/\//.test(url)) {
    console.log(`\n  ⚠ Cloud backend — if the run stalls, reduce --val-len or --put-percentage`);
  }
}

// ---------------------------------------------------------------------------
// Stats printers
// ---------------------------------------------------------------------------
function printDbStats(stats: StatsRecorder, benchStart: number) {
  const r = stats.getRelevant();
  if (!r) return;
  const putRate  = (stats.sumField(r.relevant, "puts") / r.intervalS).toFixed(1);
  const putMBs   = (stats.sumField(r.relevant, "putsBytes") / r.intervalS / 1_048_576).toFixed(3);
  const getRate  = (stats.sumField(r.relevant, "gets") / r.intervalS).toFixed(1);
  const getMBs   = (stats.sumField(r.relevant, "getsBytes") / r.intervalS / 1_048_576).toFixed(3);
  const getsTotal = stats.sumField(r.relevant, "gets");
  const hitPct   = getsTotal > 0 ? ((stats.sumField(r.relevant, "getsHits") / getsTotal) * 100).toFixed(1) : "0.0";
  const elapsed  = ((r.rangeEnd - benchStart) / 1000).toFixed(1);
  console.log(`[${elapsed}s] put/s: ${putRate} (${putMBs} MiB/s), get/s: ${getRate} (${getMBs} MiB/s), hit: ${hitPct}%, total puts: ${stats.total("puts")}, total gets: ${stats.total("gets")}`);
}

function printTxnStats(stats: StatsRecorder, benchStart: number) {
  const r = stats.getRelevant();
  if (!r) return;
  const commitRate   = (stats.sumField(r.relevant, "commits") / r.intervalS).toFixed(1);
  const abortRate    = (stats.sumField(r.relevant, "aborts") / r.intervalS).toFixed(1);
  const conflictRate = (stats.sumField(r.relevant, "conflicts") / r.intervalS).toFixed(1);
  const opsRate      = (stats.sumField(r.relevant, "totalOps") / r.intervalS).toFixed(1);
  const totalTxns    = stats.sumField(r.relevant, "commits") + stats.sumField(r.relevant, "aborts") + stats.sumField(r.relevant, "conflicts");
  const commitPct    = totalTxns > 0 ? ((stats.sumField(r.relevant, "commits") / totalTxns) * 100).toFixed(1) : "0.0";
  const abortPct     = totalTxns > 0 ? ((stats.sumField(r.relevant, "aborts") / totalTxns) * 100).toFixed(1) : "0.0";
  const conflictPct  = totalTxns > 0 ? ((stats.sumField(r.relevant, "conflicts") / totalTxns) * 100).toFixed(1) : "0.0";
  const elapsed      = ((r.rangeEnd - benchStart) / 1000).toFixed(1);
  console.log(`[${elapsed}s] commit/s: ${commitRate} (${commitPct}%), abort/s: ${abortRate} (${abortPct}%), conflict/s: ${conflictRate} (${conflictPct}%), ops/s: ${opsRate}, total: commits=${stats.total("commits")}, aborts=${stats.total("aborts")}, conflicts=${stats.total("conflicts")}, ops=${stats.total("totalOps")}`);
}

// ---------------------------------------------------------------------------
// Subcommand: db — port of db.rs
// ---------------------------------------------------------------------------
function runDbBench(argv: string[]) {
  const { values: args } = parseArgs({
    args: argv,
    options: {
      url:                 { type: "string",  default: ":memory:" },
      path:                { type: "string",  default: "/slatedb-bencher" },
      duration:            { type: "string",  default: "60" },
      concurrency:         { type: "string",  default: "4" },
      "num-rows":          { type: "string" },
      "key-len":           { type: "string",  default: "16" },
      "key-count":         { type: "string",  default: "100000" },
      "val-len":           { type: "string",  default: "1024" },
      "put-percentage":    { type: "string",  default: "20" },
      "get-hit-percentage":{ type: "string",  default: "95" },
      "await-durable":     { type: "boolean", default: false },
      "key-generator":     { type: "string",  default: "fixed-set" },
      help:                { type: "boolean", default: false },
    },
    strict: true,
  });

  if (args.help) {
    console.log(`slatedb-bencher db — mixed read/write benchmark

Options:
  --url <url>               Object store URL (default: ":memory:")
  --path <path>             DB path prefix (default: "/slatedb-bencher")
  --duration <secs>         Run duration in seconds (default: 60)
  --concurrency <n>         Number of workers (default: 4)
  --num-rows <n>            Stop after N total puts (optional)
  --key-len <bytes>         Key length in bytes (default: 16)
  --key-count <n>           Fixed-set key pool size (default: 100000)
  --val-len <bytes>         Value length in bytes (default: 1024)
  --put-percentage <0-100>  Write ratio (default: 20)
  --get-hit-percentage <0-100>  Get-hit ratio (default: 95)
  --await-durable           Wait for durable writes (default: false)
  --key-generator <type>    "fixed-set" or "random" (default: "fixed-set")
  --help                    Show this help`);
    return;
  }

  const CONCURRENCY     = parseInt(args.concurrency!);
  const DURATION_S      = parseInt(args.duration!);
  const NUM_ROWS        = args["num-rows"] ? parseInt(args["num-rows"]) : Infinity;
  const KEY_LEN         = parseInt(args["key-len"]!);
  const KEY_COUNT       = parseInt(args["key-count"]!);
  const VAL_LEN         = parseInt(args["val-len"]!);
  const PUT_PCT         = parseInt(args["put-percentage"]!);
  const GET_HIT_PCT     = parseInt(args["get-hit-percentage"]!);
  const AWAIT_DURABLE   = args["await-durable"]!;
  const KEY_GEN         = args["key-generator"]!;

  console.log("slatedb-bencher db (Bun FFI)\n");
  console.log(`  url:              ${args.url}`);
  console.log(`  path:             ${args.path}`);
  console.log(`  duration:         ${DURATION_S}s`);
  console.log(`  concurrency:      ${CONCURRENCY}`);
  console.log(`  num-rows:         ${NUM_ROWS === Infinity ? "unlimited" : NUM_ROWS}`);
  console.log(`  key-len:          ${KEY_LEN}B`);
  console.log(`  key-count:        ${KEY_COUNT}`);
  console.log(`  val-len:          ${VAL_LEN}B`);
  console.log(`  put-percentage:   ${PUT_PCT}%`);
  console.log(`  get-hit-pct:      ${GET_HIT_PCT}%`);
  console.log(`  await-durable:    ${AWAIT_DURABLE}`);
  console.log(`  key-generator:    ${KEY_GEN}`);

  cloudWarning(args.url!);
  console.log();

  const ts = Date.now();
  const dbPath = `${args.path}/bench_db_${ts}`;
  const db = SlateDB.open(dbPath, args.url!);
  const stats = new StatsRecorder();
  const benchStart = performance.now();

  for (let w = 0; w < CONCURRENCY; w++) {
    const keyGen = makeKeyGen(KEY_GEN, KEY_LEN, KEY_COUNT);
    const perWorkerDuration = DURATION_S * 1000 / CONCURRENCY;
    const perWorkerRows = NUM_ROWS === Infinity ? Infinity : Math.ceil(NUM_ROWS / CONCURRENCY);

    const start = performance.now();
    let lastReport = start;
    let lastDump = start;
    let puts = 0, gets = 0, putsBytes = 0, getsBytes = 0, getsHits = 0;

    while (stats.total("puts") < perWorkerRows * (w + 1) && (performance.now() - start) < perWorkerDuration) {
      if (Math.floor(Math.random() * 100) < PUT_PCT) {
        const key = keyGen.nextKey();
        const value = randomBytes(VAL_LEN);
        try {
          db.put(key, value, AWAIT_DURABLE);
          puts++;
          putsBytes += VAL_LEN;
        } catch { /* ignore */ }
      } else {
        const key = Math.floor(Math.random() * 100) < GET_HIT_PCT
          ? keyGen.usedKey()
          : keyGen.nextKey();
        try {
          const val = db.get(key);
          gets++;
          getsHits += val !== null ? 1 : 0;
          getsBytes += key.byteLength + (val?.byteLength ?? 0);
        } catch { /* ignore */ }
      }

      const now = performance.now();
      if (now - lastReport >= REPORT_INTERVAL_MS) {
        lastReport = now;
        stats.record(now, { puts, gets, putsBytes, getsBytes, getsHits });
        puts = gets = putsBytes = getsBytes = getsHits = 0;

        // Print stats periodically (every STAT_DUMP_INTERVAL_MS)
        if (now - lastDump >= STAT_DUMP_INTERVAL_MS) {
          lastDump = now;
          printDbStats(stats, benchStart);
        }
      }
    }

    if (puts || gets) stats.record(performance.now(), { puts, gets, putsBytes, getsBytes, getsHits });
    printDbStats(stats, benchStart);
  }

  const benchS = ((performance.now() - benchStart) / 1000).toFixed(1);
  console.log(`\nBench done in ${benchS}s — total puts: ${stats.total("puts")}, total gets: ${stats.total("gets")}`);

  process.stdout.write("Flushing... ");
  const t0 = performance.now();
  db.flush();
  const flushMs = (performance.now() - t0).toFixed(0);
  process.stdout.write(`${flushMs}ms. Closing... `);
  const t1 = performance.now();
  db.close();
  const closeMs = (performance.now() - t1).toFixed(0);
  console.log(`${closeMs}ms.`);
}

// ---------------------------------------------------------------------------
// Subcommand: transaction — port of transactions.rs
// ---------------------------------------------------------------------------
function runTransactionBench(argv: string[]) {
  const { values: args } = parseArgs({
    args: argv,
    options: {
      url:                 { type: "string",  default: ":memory:" },
      path:                { type: "string",  default: "/slatedb-bencher" },
      duration:            { type: "string",  default: "60" },
      concurrency:         { type: "string",  default: "4" },
      "key-len":           { type: "string",  default: "16" },
      "key-count":         { type: "string",  default: "100000" },
      "val-len":           { type: "string",  default: "1024" },
      "transaction-size":  { type: "string",  default: "10" },
      "abort-percentage":  { type: "string",  default: "10" },
      "use-write-batch":   { type: "boolean", default: false },
      "isolation-level":   { type: "string",  default: "snapshot" },
      "await-durable":     { type: "boolean", default: false },
      "key-generator":     { type: "string",  default: "fixed-set" },
      help:                { type: "boolean", default: false },
    },
    strict: true,
  });

  if (args.help) {
    console.log(`slatedb-bencher transaction — transaction benchmark

Options:
  --url <url>               Object store URL (default: ":memory:")
  --path <path>             DB path prefix (default: "/slatedb-bencher")
  --duration <secs>         Run duration in seconds (default: 60)
  --concurrency <n>         Number of workers (default: 4)
  --key-len <bytes>         Key length in bytes (default: 16)
  --key-count <n>           Fixed-set key pool size (default: 100000)
  --val-len <bytes>         Value length in bytes (default: 1024)
  --transaction-size <n>    Operations per transaction (default: 10)
  --abort-percentage <0-100> Percentage to abort/rollback (default: 10)
  --use-write-batch         Use WriteBatch instead of Transaction (default: false)
  --isolation-level <level> "snapshot" or "serializable" (default: "snapshot")
  --await-durable           Wait for durable writes (default: false)
  --key-generator <type>    "fixed-set" or "random" (default: "fixed-set")
  --help                    Show this help`);
    return;
  }

  const CONCURRENCY     = parseInt(args.concurrency!);
  const DURATION_S      = parseInt(args.duration!);
  const KEY_LEN         = parseInt(args["key-len"]!);
  const KEY_COUNT       = parseInt(args["key-count"]!);
  const VAL_LEN         = parseInt(args["val-len"]!);
  const TXN_SIZE        = parseInt(args["transaction-size"]!);
  const ABORT_PCT       = parseInt(args["abort-percentage"]!);
  const USE_BATCH       = args["use-write-batch"]!;
  const ISOLATION       = args["isolation-level"] === "serializable"
                            ? IsolationLevel.SerializableSnapshot
                            : IsolationLevel.Snapshot;
  const AWAIT_DURABLE   = args["await-durable"]!;
  const KEY_GEN         = args["key-generator"]!;

  console.log("slatedb-bencher transaction (Bun FFI)\n");
  console.log(`  url:              ${args.url}`);
  console.log(`  path:             ${args.path}`);
  console.log(`  duration:         ${DURATION_S}s`);
  console.log(`  concurrency:      ${CONCURRENCY}`);
  console.log(`  key-len:          ${KEY_LEN}B`);
  console.log(`  key-count:        ${KEY_COUNT}`);
  console.log(`  val-len:          ${VAL_LEN}B`);
  console.log(`  txn-size:         ${TXN_SIZE} ops`);
  console.log(`  abort-pct:        ${ABORT_PCT}%`);
  console.log(`  use-write-batch:  ${USE_BATCH}`);
  console.log(`  isolation:        ${args["isolation-level"]}`);
  console.log(`  await-durable:    ${AWAIT_DURABLE}`);
  console.log(`  key-generator:    ${KEY_GEN}`);
  cloudWarning(args.url!);
  console.log();

  const ts = Date.now();
  const dbPath = `${args.path}/bench_txn_${ts}`;
  const db = SlateDB.open(dbPath, args.url!);
  const stats = new StatsRecorder();
  const benchStart = performance.now();

  for (let w = 0; w < CONCURRENCY; w++) {
    const keyGen = makeKeyGen(KEY_GEN, KEY_LEN, KEY_COUNT);
    const perWorkerDuration = DURATION_S * 1000 / CONCURRENCY;

    const start = performance.now();
    let lastReport = start;
    let lastDump = start;
    let commits = 0, aborts = 0, conflicts = 0, totalOps = 0;

    while ((performance.now() - start) < perWorkerDuration) {
      const shouldAbort = Math.floor(Math.random() * 100) < ABORT_PCT;

      if (USE_BATCH) {
        // WriteBatch path — matches Rust: abort returns Ok(0) → counts as commit
        if (shouldAbort) {
          commits++;
        } else {
          const batch = new WriteBatch();
          for (let i = 0; i < TXN_SIZE; i++) {
            const key = keyGen.nextKey();
            const value = randomBytes(VAL_LEN);
            batch.put(key, value);
          }
          try {
            db.writeBatch(batch, AWAIT_DURABLE);
            commits++;
            totalOps += TXN_SIZE;
          } catch {
            conflicts++;
          }
        }
      } else {
        // Transaction path
        let txn: Transaction;
        try {
          txn = db.begin(ISOLATION);
        } catch {
          conflicts++;
          continue;
        }

        let txnOk = true;
        for (let i = 0; i < TXN_SIZE; i++) {
          const key = keyGen.nextKey();
          const value = randomBytes(VAL_LEN);
          try {
            txn.put(key, value);
          } catch {
            txn.rollback();
            txnOk = false;
            aborts++;
            break;
          }
        }

        if (!txnOk) continue;

        if (shouldAbort) {
          txn.rollback();
          aborts++;
        } else {
          try {
            txn.commit(AWAIT_DURABLE);
            commits++;
            totalOps += TXN_SIZE;
          } catch {
            conflicts++;
          }
        }
      }

      const now = performance.now();
      if (now - lastReport >= REPORT_INTERVAL_MS) {
        lastReport = now;
        stats.record(now, { commits, aborts, conflicts, totalOps });
        commits = aborts = conflicts = totalOps = 0;

        // Print stats periodically (every STAT_DUMP_INTERVAL_MS)
        if (now - lastDump >= STAT_DUMP_INTERVAL_MS) {
          lastDump = now;
          printTxnStats(stats, benchStart);
        }
      }
    }

    if (commits || aborts || conflicts) {
      stats.record(performance.now(), { commits, aborts, conflicts, totalOps });
    }
    printTxnStats(stats, benchStart);
  }

  const benchS = ((performance.now() - benchStart) / 1000).toFixed(1);
  console.log(`\nBench done in ${benchS}s — commits: ${stats.total("commits")}, aborts: ${stats.total("aborts")}, conflicts: ${stats.total("conflicts")}, ops: ${stats.total("totalOps")}`);

  process.stdout.write("Flushing... ");
  const t0 = performance.now();
  db.flush();
  const flushMs = (performance.now() - t0).toFixed(0);
  process.stdout.write(`${flushMs}ms. Closing... `);
  const t1 = performance.now();
  db.close();
  const closeMs = (performance.now() - t1).toFixed(0);
  console.log(`${closeMs}ms.`);
}

// ---------------------------------------------------------------------------
// Main — subcommand dispatch
// ---------------------------------------------------------------------------
const subcommand = process.argv[2];

if (!subcommand || subcommand === "--help" || subcommand === "-h") {
  console.log(`slatedb-bencher — port of slatedb-bencher (Bun FFI)

Subcommands:
  db            Mixed read/write benchmark (put + get)
  transaction   Transaction benchmark (begin/commit/rollback)

Usage:
  bun run bencher.ts db [options]
  bun run bencher.ts transaction [options]
  bun run bencher.ts <subcommand> --help   Show subcommand options

Note: The \`compaction\` subcommand is not ported — it requires
internal Rust APIs (CompactionExecuteBench) not exposed through FFI.`);
  process.exit(0);
}

const subArgv = process.argv.slice(3);

switch (subcommand) {
  case "db":
    runDbBench(subArgv);
    break;
  case "transaction":
    runTransactionBench(subArgv);
    break;
  default:
    console.error(`Unknown subcommand: ${subcommand}`);
    console.error(`Run "bun run bencher.ts --help" for usage.`);
    process.exit(1);
}
