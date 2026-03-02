/**
 * Micro-benchmark — port of SlateDB's `benches/db_operations.rs` (Criterion).
 *
 * Measures the same two operations:
 *   1. put  (await_durable=false, 1000 samples)
 *   2. open_close
 *
 * Plus additional bridge-specific benchmarks:
 *   3. get  (hot key)
 *   4. scan (full, 100 keys)
 *
 * @see https://github.com/slatedb/slatedb/blob/main/slatedb/benches/db_operations.rs
 */
import { SlateDB } from "./index";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------
interface BenchResult {
  name: string;
  samples: number;
  totalMs: number;
  avgUs: number;
  minUs: number;
  maxUs: number;
  p50Us: number;
  p99Us: number;
  opsPerSec: number;
}

function bench(
  name: string,
  fn: () => void,
  samples: number,
  warmup = 50,
): BenchResult {
  // warmup
  for (let i = 0; i < warmup; i++) fn();

  const times: number[] = new Array(samples);
  const t0 = performance.now();
  for (let i = 0; i < samples; i++) {
    const start = performance.now();
    fn();
    times[i] = (performance.now() - start) * 1000; // μs
  }
  const totalMs = performance.now() - t0;

  times.sort((a, b) => a - b);
  const sum = times.reduce((a, b) => a + b, 0);

  return {
    name,
    samples,
    totalMs,
    avgUs: sum / samples,
    minUs: times[0],
    maxUs: times[samples - 1],
    p50Us: times[Math.floor(samples * 0.5)],
    p99Us: times[Math.floor(samples * 0.99)],
    opsPerSec: Math.round((samples / totalMs) * 1000),
  };
}

function report(r: BenchResult) {
  const pad = (s: string, n: number) => s.padStart(n);
  const us = (v: number) => pad(v.toFixed(2), 10) + " μs";
  console.log(`  ${r.name}`);
  console.log(`    samples:  ${r.samples}`);
  console.log(`    avg:     ${us(r.avgUs)}`);
  console.log(`    min:     ${us(r.minUs)}`);
  console.log(`    max:     ${us(r.maxUs)}`);
  console.log(`    p50:     ${us(r.p50Us)}`);
  console.log(`    p99:     ${us(r.p99Us)}`);
  console.log(`    ops/sec:  ${r.opsPerSec.toLocaleString()}`);
  console.log();
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------
console.log("SlateDB Bun FFI — micro-benchmarks\n");
console.log("(ported from slatedb/benches/db_operations.rs)\n");

// -- 1. put (matches Criterion bench exactly) --
{
  const db = SlateDB.open("/tmp/bench_put", ":memory:");
  report(
    bench(
      "put (await_durable=false)",
      () => db.put("key", "value", false),
      1_000,
    ),
  );
  db.close();
}

// -- 2. open_close (matches Criterion bench exactly) --
{
  report(
    bench(
      "open_close",
      () => {
        const db = SlateDB.open("/tmp/bench_open_close", ":memory:");
        db.close();
      },
      1_000,
    ),
  );
}

// -- 3. get (bridge-specific: measures FFI round-trip for reads) --
{
  const db = SlateDB.open("/tmp/bench_get", ":memory:");
  db.put("key", "value");
  report(
    bench(
      "get (hot key)",
      () => db.get("key"),
      1_000,
    ),
  );
  db.close();
}

// -- 4. scan (bridge-specific: measures flat-buffer serialization overhead) --
{
  const db = SlateDB.open("/tmp/bench_scan", ":memory:");
  for (let i = 0; i < 100; i++) {
    db.put(`key_${String(i).padStart(4, "0")}`, `val_${"x".repeat(100)}`, false);
  }
  db.flush();
  report(
    bench(
      "scan (100 keys, ~100B values)",
      () => db.scan(),
      10_000,
    ),
  );
  db.close();
}
