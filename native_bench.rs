//! Native Rust micro-bench — matches bench.ts setup exactly.
//! Run via: cargo run --release --example native_bench

use slatedb::config::{PutOptions, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

fn bench(name: &str, samples: usize, warmup: usize, mut f: impl FnMut()) {
    for _ in 0..warmup {
        f();
    }

    let mut times = Vec::with_capacity(samples);
    let t0 = Instant::now();
    for _ in 0..samples {
        let start = Instant::now();
        f();
        times.push(start.elapsed().as_nanos() as f64 / 1000.0);
    }
    let total_ms = t0.elapsed().as_millis();

    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let sum: f64 = times.iter().sum();
    let avg = sum / samples as f64;
    let p50 = times[samples / 2];
    let p99 = times[(samples as f64 * 0.99) as usize];
    let ops = (samples as f64 / total_ms as f64) * 1000.0;

    println!("  {name}");
    println!("    samples:  {samples}");
    println!("    avg:      {avg:10.2} μs");
    println!("    min:      {:10.2} μs", times[0]);
    println!("    max:      {:10.2} μs", times[samples - 1]);
    println!("    p50:      {p50:10.2} μs");
    println!("    p99:      {p99:10.2} μs");
    println!("    ops/sec:  {ops:.0}");
    println!();
}

fn main() {
    let rt = Runtime::new().unwrap();

    println!("SlateDB native Rust — micro-benchmarks\n");

    // -- put --
    {
        let db = rt
            .block_on(Db::open("/tmp/native_bench_put", Arc::new(InMemory::new())))
            .unwrap();
        let wo = WriteOptions {
            await_durable: false,
        };
        bench("put (await_durable=false)", 1_000, 50, || {
            rt.block_on(
                db.put_with_options(b"key", b"value", &PutOptions::default(), &wo),
            )
            .unwrap();
        });
        rt.block_on(db.close()).unwrap();
    }

    // -- get --
    {
        let db = rt
            .block_on(Db::open("/tmp/native_bench_get", Arc::new(InMemory::new())))
            .unwrap();
        rt.block_on(db.put(b"key", b"value")).unwrap();
        bench("get (hot key)", 1_000, 50, || {
            rt.block_on(db.get(b"key")).unwrap();
        });
        rt.block_on(db.close()).unwrap();
    }

    // -- scan --
    {
        let db = rt
            .block_on(Db::open(
                "/tmp/native_bench_scan",
                Arc::new(InMemory::new()),
            ))
            .unwrap();
        let wo = WriteOptions {
            await_durable: false,
        };
        for i in 0..100u32 {
            let key = format!("key_{i:04}");
            let val = "x".repeat(100);
            rt.block_on(db.put_with_options(
                key.as_bytes(),
                val.as_bytes(),
                &PutOptions::default(),
                &wo,
            ))
            .unwrap();
        }
        rt.block_on(db.flush()).unwrap();

        bench("scan (100 keys, ~100B values)", 10_000, 50, || {
            rt.block_on(async {
                let mut iter = db.scan::<Vec<u8>, _>(..).await.unwrap();
                while let Ok(Some(_)) = iter.next().await {}
            });
        });
        rt.block_on(db.close()).unwrap();
    }
}
