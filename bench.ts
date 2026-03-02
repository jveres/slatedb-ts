/**
 * Micro-benchmark — port of SlateDB's `benches/db_operations.rs` (Criterion).
 *
 * Measures:
 *   1. put  (await_durable=false)
 *   2. open_close
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

async function bench(
  name: string,
  fn: () => Promise<void>,
  samples: number,
  warmup = 50,
): Promise<BenchResult> {
  for (let i = 0; i < warmup; i++) await fn();

  const times: number[] = new Array(samples);
  const t0 = performance.now();
  for (let i = 0; i < samples; i++) {
    const start = performance.now();
    await fn();
    times[i] = (performance.now() - start) * 1000;
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
console.log("SlateDB napi-rs — micro-benchmarks\n");
console.log("(ported from slatedb/benches/db_operations.rs)\n");

const key = Buffer.from("key");
const value = Buffer.from("value");

// -- 1. put --
{
  const db = await SlateDB.open("/tmp/bench_put", ":memory:");
  report(
    await bench("put (await_durable=false)", () => db.put(key, value, false), 1_000),
  );
  await db.close();
}

// -- 2. open_close --
{
  report(
    await bench(
      "open_close",
      async () => {
        const db = await SlateDB.open("/tmp/bench_open_close", ":memory:");
        await db.close();
      },
      1_000,
    ),
  );
}

// -- 3. get --
{
  const db = await SlateDB.open("/tmp/bench_get", ":memory:");
  await db.put(key, value);
  report(await bench("get (hot key)", () => db.get(key), 1_000));
  await db.close();
}

// -- 4. scan --
{
  const db = await SlateDB.open("/tmp/bench_scan", ":memory:");
  for (let i = 0; i < 100; i++) {
    await db.put(
      Buffer.from(`key_${String(i).padStart(4, "0")}`),
      Buffer.from(`val_${"x".repeat(100)}`),
      false,
    );
  }
  await db.flush();
  report(await bench("scan (100 keys, ~100B values)", () => db.scan(), 1_000));
  await db.close();
}
