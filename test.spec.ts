/**
 * Integration test — faithful port of SlateDB's `examples/src/full_example.rs`.
 *
 * Now fully async via napi-rs. All operations return Promises —
 * no blocking the main thread, no backpressure stalls.
 *
 * @see https://github.com/slatedb/slatedb/blob/main/examples/src/full_example.rs
 * @see https://github.com/slatedb/slatedb/blob/main/slatedb/tests/db.rs
 */
import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import {
  SlateDB,
  WriteBatch,
  IsolationLevel,
  DurabilityLevel,
  FlushType,
  CheckpointScope,
} from "./index";

const urls = (process.env.SLATEDB_TEST_URLS ?? ":memory:")
  .split(",")
  .map((s: string) => s.trim())
  .filter(Boolean);

const dec = new TextDecoder();
const str = (b: Buffer | Uint8Array) => dec.decode(b);

function uniquePath(tag: string): string {
  return `/tmp/slatedb_ts_${tag}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

for (const url of urls) {
  const label = url === ":memory:" ? "memory" : url;

  describe(`[${label}] full_example.rs`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("full"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("put then get returns the value", async () => {
      await db.put(Buffer.from("test_key"), Buffer.from("test_value"), false);
      const val = await db.get(Buffer.from("test_key"));
      expect(str(val)).toBe("test_value");
    });

    test("delete removes the key", async () => {
      await db.put(Buffer.from("del_key"), Buffer.from("del_value"), false);
      expect(str(await db.get(Buffer.from("del_key")))).toBe("del_value");

      await db.delete(Buffer.from("del_key"), false);
      expect(await db.get(Buffer.from("del_key"))).toBeNull();
    });

    test("scan over unbounded range returns all keys in sorted order", async () => {
      await db.put(Buffer.from("scan_key1"), Buffer.from("scan_value1"), false);
      await db.put(Buffer.from("scan_key2"), Buffer.from("scan_value2"), false);
      await db.put(Buffer.from("scan_key3"), Buffer.from("scan_value3"), false);
      await db.put(Buffer.from("scan_key4"), Buffer.from("scan_value4"), false);

      const items = await db.scan(Buffer.from("scan_key1"), Buffer.from("scan_key5"));
      expect(items).toHaveLength(4);

      for (let i = 0; i < items.length; i++) {
        const n = i + 1;
        expect(str(items[i].key)).toBe(`scan_key${n}`);
        expect(str(items[i].value)).toBe(`scan_value${n}`);
      }
    });

    test("scan over bounded range [key1, key3) returns matching keys", async () => {
      const items = await db.scan(Buffer.from("scan_key1"), Buffer.from("scan_key3"));
      expect(items).toHaveLength(2);
      expect(str(items[0].key)).toBe("scan_key1");
      expect(str(items[1].key)).toBe("scan_key2");
    });

    test("scan with start bound returns keys from that point", async () => {
      const items = await db.scan(Buffer.from("scan_key4"), Buffer.from("scan_key5"));
      expect(items).toHaveLength(1);
      expect(str(items[0].key)).toBe("scan_key4");
    });
  });

  describe(`[${label}] write batch`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("batch"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("atomic multi-put via WriteBatch", async () => {
      const batch = new WriteBatch();
      await batch.put(Buffer.from("b1"), Buffer.from("v1"));
      await batch.put(Buffer.from("b2"), Buffer.from("v2"));
      await batch.put(Buffer.from("b3"), Buffer.from("v3"));
      await db.writeBatch(batch, false);

      expect(str(await db.get(Buffer.from("b1")))).toBe("v1");
      expect(str(await db.get(Buffer.from("b2")))).toBe("v2");
      expect(str(await db.get(Buffer.from("b3")))).toBe("v3");
    });

    test("WriteBatch with deletes", async () => {
      await db.put(Buffer.from("bd1"), Buffer.from("before"), false);
      await db.put(Buffer.from("bd2"), Buffer.from("keep"), false);

      const batch = new WriteBatch();
      await batch.delete(Buffer.from("bd1"));
      await batch.put(Buffer.from("bd3"), Buffer.from("new"));
      await db.writeBatch(batch, false);

      expect(await db.get(Buffer.from("bd1"))).toBeNull();
      expect(str(await db.get(Buffer.from("bd2")))).toBe("keep");
      expect(str(await db.get(Buffer.from("bd3")))).toBe("new");
    });

    test("WriteBatch non-durable", async () => {
      const batch = new WriteBatch();
      await batch.put(Buffer.from("bnd1"), Buffer.from("fast"));
      await db.writeBatch(batch, false);

      expect(str(await db.get(Buffer.from("bnd1")))).toBe("fast");
    });

    test("free unused WriteBatch without error", async () => {
      const batch = new WriteBatch();
      await batch.put(Buffer.from("unused"), Buffer.from("data"));
      await batch.free();
    });
  });

  describe(`[${label}] transactions`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("txn"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("transaction commit makes writes visible", async () => {
      const txn = await db.begin();
      await txn.put(Buffer.from("tk1"), Buffer.from("tv1"));
      await txn.put(Buffer.from("tk2"), Buffer.from("tv2"));
      await txn.commit(false);

      expect(str(await db.get(Buffer.from("tk1")))).toBe("tv1");
      expect(str(await db.get(Buffer.from("tk2")))).toBe("tv2");
    });

    test("transaction rollback discards writes", async () => {
      await db.put(Buffer.from("tr_existing"), Buffer.from("original"), false);

      const txn = await db.begin();
      await txn.put(Buffer.from("tr_existing"), Buffer.from("modified"));
      await txn.put(Buffer.from("tr_new_key"), Buffer.from("new_val"));
      await txn.rollback();

      expect(str(await db.get(Buffer.from("tr_existing")))).toBe("original");
      expect(await db.get(Buffer.from("tr_new_key"))).toBeNull();
    });

    test("transaction read-your-writes", async () => {
      await db.put(Buffer.from("ryw"), Buffer.from("before"), false);

      const txn = await db.begin();
      await txn.put(Buffer.from("ryw"), Buffer.from("inside_txn"));
      const val = await txn.get(Buffer.from("ryw"));
      expect(str(val)).toBe("inside_txn");
      await txn.commit(false);

      expect(str(await db.get(Buffer.from("ryw")))).toBe("inside_txn");
    });

    test("transaction delete", async () => {
      await db.put(Buffer.from("td"), Buffer.from("will_delete"), false);

      const txn = await db.begin();
      await txn.delete(Buffer.from("td"));
      expect(await txn.get(Buffer.from("td"))).toBeNull();
      await txn.commit(false);

      expect(await db.get(Buffer.from("td"))).toBeNull();
    });

    test("transaction commit non-durable", async () => {
      const txn = await db.begin();
      await txn.put(Buffer.from("nd_txn"), Buffer.from("fast"));
      await txn.commit(false);

      expect(str(await db.get(Buffer.from("nd_txn")))).toBe("fast");
    });

    test("transaction with SerializableSnapshot isolation", async () => {
      const txn = await db.begin(IsolationLevel.SerializableSnapshot);
      await txn.put(Buffer.from("iso"), Buffer.from("serializable"));
      await txn.commit(false);

      expect(str(await db.get(Buffer.from("iso")))).toBe("serializable");
    });
  });

  describe(`[${label}] bridge extras`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("extra"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("binary keys and values round-trip", async () => {
      const key = Buffer.from([0xCA, 0xFE]);
      const val = Buffer.from([0xBE, 0xEF]);
      await db.put(key, val, false);

      const got = await db.get(key);
      expect(got).not.toBeNull();
      expect(got[0]).toBe(0xBE);
      expect(got[1]).toBe(0xEF);
    });

    test("get on missing key returns null", async () => {
      expect(await db.get(Buffer.from("no_such_key"))).toBeNull();
    });

    test("put empty value", async () => {
      await db.put(Buffer.from("empty"), Buffer.from([]), false);
      const got = await db.get(Buffer.from("empty"));
      expect(got).not.toBeNull();
      expect(got.byteLength).toBe(0);
    });

    test("overwrite replaces value", async () => {
      await db.put(Buffer.from("ow_k"), Buffer.from("v1"), false);
      expect(str(await db.get(Buffer.from("ow_k")))).toBe("v1");
      await db.put(Buffer.from("ow_k"), Buffer.from("v2"), false);
      expect(str(await db.get(Buffer.from("ow_k")))).toBe("v2");
    });

    test("delete non-existent key is a no-op", async () => {
      await db.delete(Buffer.from("ghost"), false);
    });

    test("scan empty prefix returns empty array", async () => {
      const items = await db.scan(Buffer.from("zzz_no_match"), Buffer.from("zzz_no_match~"));
      expect(items).toEqual([]);
    });

    test("flush does not lose data", async () => {
      await db.put(Buffer.from("fl1"), Buffer.from("v1"), false);
      await db.flush();
      expect(str(await db.get(Buffer.from("fl1")))).toBe("v1");
    });

    test("many sequential writes maintain scan order", async () => {
      const n = 100;
      for (let i = 0; i < n; i++) {
        const key = `seq_${String(i).padStart(4, "0")}`;
        await db.put(Buffer.from(key), Buffer.from(`val_${i}`), false);
      }
      await db.flush();

      const items = await db.scan(Buffer.from("seq_"), Buffer.from("seq_~"));
      expect(items).toHaveLength(n);

      for (let i = 1; i < items.length; i++) {
        const prev = str(items[i - 1].key);
        const curr = str(items[i].key);
        expect(prev < curr).toBe(true);
      }
    });
  });

  // =========================================================================
  // scan_prefix
  // =========================================================================
  describe(`[${label}] scan_prefix`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("prefix"), url);
      await db.put(Buffer.from("user:1"), Buffer.from("alice"), false);
      await db.put(Buffer.from("user:2"), Buffer.from("bob"), false);
      await db.put(Buffer.from("user:3"), Buffer.from("carol"), false);
      await db.put(Buffer.from("order:1"), Buffer.from("o1"), false);
      await db.put(Buffer.from("order:2"), Buffer.from("o2"), false);
    });

    afterAll(async () => {
      await db.close();
    });

    test("scanPrefix returns only matching keys", async () => {
      const items = await db.scanPrefix(Buffer.from("user:"));
      expect(items).toHaveLength(3);
      expect(str(items[0].key)).toBe("user:1");
      expect(str(items[2].key)).toBe("user:3");
    });

    test("scanPrefix with no matches returns empty array", async () => {
      const items = await db.scanPrefix(Buffer.from("zzz:"));
      expect(items).toEqual([]);
    });

    test("scanPrefix for different prefix", async () => {
      const items = await db.scanPrefix(Buffer.from("order:"));
      expect(items).toHaveLength(2);
    });
  });

  // =========================================================================
  // snapshot
  // =========================================================================
  describe(`[${label}] snapshot`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("snap"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("snapshot sees data at the point it was created", async () => {
      await db.put(Buffer.from("snap_k1"), Buffer.from("v1"), false);
      await db.put(Buffer.from("snap_k2"), Buffer.from("v2"), false);

      const snap = await db.snapshot();

      // Write after snapshot
      await db.put(Buffer.from("snap_k1"), Buffer.from("v1_updated"), false);
      await db.put(Buffer.from("snap_k3"), Buffer.from("v3"), false);

      // Snapshot reads should see original values
      expect(str(await snap.get(Buffer.from("snap_k1")))).toBe("v1");
      expect(str(await snap.get(Buffer.from("snap_k2")))).toBe("v2");
      // snap_k3 didn't exist at snapshot time
      expect(await snap.get(Buffer.from("snap_k3"))).toBeNull();

      // DB reads should see updated values
      expect(str(await db.get(Buffer.from("snap_k1")))).toBe("v1_updated");
      expect(str(await db.get(Buffer.from("snap_k3")))).toBe("v3");
    });

    test("snapshot scan returns point-in-time data", async () => {
      await db.put(Buffer.from("ss_a"), Buffer.from("1"), false);
      await db.put(Buffer.from("ss_b"), Buffer.from("2"), false);

      const snap = await db.snapshot();
      await db.put(Buffer.from("ss_c"), Buffer.from("3"), false);

      const items = await snap.scan(Buffer.from("ss_"), Buffer.from("ss_~"));
      expect(items).toHaveLength(2);
      expect(str(items[0].key)).toBe("ss_a");
      expect(str(items[1].key)).toBe("ss_b");
    });

    test("snapshot scanPrefix", async () => {
      const snap = await db.snapshot();
      const items = await snap.scanPrefix(Buffer.from("ss_"));
      expect(items.length).toBeGreaterThanOrEqual(2);
    });

    test("snapshot getString", async () => {
      await db.put(Buffer.from("snap_str"), Buffer.from("hello"), false);
      const snap = await db.snapshot();
      expect(await snap.getString(Buffer.from("snap_str"))).toBe("hello");
    });
  });

  // =========================================================================
  // merge operators
  // =========================================================================
  describe(`[${label}] merge — string_concat`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("merge_str"), url, {
        mergeOperator: "string_concat",
      });
    });

    afterAll(async () => {
      await db.close();
    });

    test("merge concatenates values", async () => {
      await db.put(Buffer.from("mc1"), Buffer.from("hello"), false);
      await db.merge(Buffer.from("mc1"), Buffer.from(" world"), false);
      expect(await db.getString(Buffer.from("mc1"))).toBe("hello world");
    });

    test("merge on non-existent key creates it", async () => {
      await db.merge(Buffer.from("mc_new"), Buffer.from("fresh"), false);
      expect(await db.getString(Buffer.from("mc_new"))).toBe("fresh");
    });

    test("multiple merges accumulate", async () => {
      await db.merge(Buffer.from("mc_multi"), Buffer.from("a"), false);
      await db.merge(Buffer.from("mc_multi"), Buffer.from("b"), false);
      await db.merge(Buffer.from("mc_multi"), Buffer.from("c"), false);
      expect(await db.getString(Buffer.from("mc_multi"))).toBe("abc");
    });
  });

  describe(`[${label}] merge — uint64_add`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("merge_u64"), url, {
        mergeOperator: "uint64_add",
      });
    });

    afterAll(async () => {
      await db.close();
    });

    const u64 = (n: number) => {
      const buf = Buffer.alloc(8);
      buf.writeBigUInt64LE(BigInt(n));
      return buf;
    };
    const readU64 = (buf: Buffer) => Number(Buffer.from(buf).readBigUInt64LE());

    test("merge adds u64 counters", async () => {
      await db.put(Buffer.from("counter"), u64(100), false);
      await db.merge(Buffer.from("counter"), u64(5), false);
      await db.merge(Buffer.from("counter"), u64(10), false);
      expect(readU64(await db.get(Buffer.from("counter")))).toBe(115);
    });

    test("merge on non-existent key starts from zero", async () => {
      await db.merge(Buffer.from("cnt_new"), u64(42), false);
      expect(readU64(await db.get(Buffer.from("cnt_new")))).toBe(42);
    });
  });

  // =========================================================================
  // merge in WriteBatch
  // =========================================================================
  describe(`[${label}] WriteBatch merge`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("batch_merge"), url, {
        mergeOperator: "string_concat",
      });
    });

    afterAll(async () => {
      await db.close();
    });

    test("WriteBatch with merge operations", async () => {
      await db.put(Buffer.from("bm1"), Buffer.from("x"), false);

      const batch = new WriteBatch();
      await batch.merge(Buffer.from("bm1"), Buffer.from("y"));
      await batch.merge(Buffer.from("bm1"), Buffer.from("z"));
      await db.writeBatch(batch, false);

      expect(await db.getString(Buffer.from("bm1"))).toBe("xyz");
    });
  });

  // =========================================================================
  // transaction scan / scanPrefix / merge
  // =========================================================================
  describe(`[${label}] transaction scan`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("txn_scan"), url, {
        mergeOperator: "string_concat",
      });
      await db.put(Buffer.from("ts:a"), Buffer.from("1"), false);
      await db.put(Buffer.from("ts:b"), Buffer.from("2"), false);
      await db.put(Buffer.from("ts:c"), Buffer.from("3"), false);
    });

    afterAll(async () => {
      await db.close();
    });

    test("transaction scan sees own writes", async () => {
      const txn = await db.begin();
      await txn.put(Buffer.from("ts:d"), Buffer.from("4"));
      const items = await txn.scan(Buffer.from("ts:"), Buffer.from("ts:~"));
      expect(items).toHaveLength(4);
      expect(str(items[3].key)).toBe("ts:d");
      await txn.commit(false);
    });

    test("transaction scanPrefix", async () => {
      const txn = await db.begin();
      const items = await txn.scanPrefix(Buffer.from("ts:"));
      expect(items.length).toBeGreaterThanOrEqual(4);
      await txn.rollback();
    });

    test("transaction merge", async () => {
      await db.put(Buffer.from("tm"), Buffer.from("base"), false);
      const txn = await db.begin();
      await txn.merge(Buffer.from("tm"), Buffer.from("+ext"));
      const val = await txn.getString(Buffer.from("tm"));
      expect(val).toBe("base+ext");
      await txn.commit(false);
      expect(await db.getString(Buffer.from("tm"))).toBe("base+ext");
    });
  });

  // =========================================================================
  // flush types
  // =========================================================================
  describe(`[${label}] flush options`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("flush_opts"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("flush with WAL type", async () => {
      await db.put(Buffer.from("fw1"), Buffer.from("v1"), false);
      await db.flush(FlushType.Wal);
      expect(str(await db.get(Buffer.from("fw1")))).toBe("v1");
    });

    test("flush with MemTable type", async () => {
      await db.put(Buffer.from("fm1"), Buffer.from("v1"), false);
      await db.flush(FlushType.MemTable);
      expect(str(await db.get(Buffer.from("fm1")))).toBe("v1");
    });
  });

  // =========================================================================
  // metrics
  // =========================================================================
  describe(`[${label}] metrics`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("metrics"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("metrics returns an array of { name, value }", async () => {
      await db.put(Buffer.from("m1"), Buffer.from("v1"), false);
      const metrics = db.metrics();
      expect(Array.isArray(metrics)).toBe(true);
      expect(metrics.length).toBeGreaterThan(0);
      expect(metrics[0]).toHaveProperty("name");
      expect(metrics[0]).toHaveProperty("value");
      expect(typeof metrics[0].name).toBe("string");
      expect(typeof metrics[0].value).toBe("number");
    });
  });

  // =========================================================================
  // checkpoint
  // =========================================================================
  describe(`[${label}] checkpoint`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("ckpt"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("createCheckpoint returns id and manifestId", async () => {
      await db.put(Buffer.from("cp1"), Buffer.from("v1"), false);
      await db.flush();
      const result = await db.createCheckpoint();
      expect(result).toHaveProperty("id");
      expect(result).toHaveProperty("manifestId");
      expect(typeof result.id).toBe("string");
      expect(result.id.length).toBeGreaterThan(0);
      expect(typeof result.manifestId).toBe("number");
    });

    test("createCheckpoint with name and lifetime", async () => {
      const result = await db.createCheckpoint(
        CheckpointScope.Durable,
        60000, // 60s lifetime
        "test-checkpoint",
      );
      expect(result.id.length).toBeGreaterThan(0);
    });
  });

  // =========================================================================
  // open with settings
  // =========================================================================
  describe(`[${label}] open with settings`, () => {
    test("custom flush interval and memtable size", async () => {
      const db = await SlateDB.open(uniquePath("settings"), url, {
        flushIntervalMs: 500,
        l0SstSizeBytes: 1024 * 1024,
      });
      await db.put(Buffer.from("s1"), Buffer.from("v1"), false);
      expect(str(await db.get(Buffer.from("s1")))).toBe("v1");
      await db.close();
    });

    test("unknown merge operator throws", async () => {
      try {
        await SlateDB.open(uniquePath("bad_merge"), url, {
          mergeOperator: "nonexistent" as any,
        });
        expect(true).toBe(false); // should not reach
      } catch (e: any) {
        expect(e.message).toContain("unknown merge operator");
      }
    });
  });

  // =========================================================================
  // getString convenience
  // =========================================================================
  describe(`[${label}] getString`, () => {
    let db: any;

    beforeAll(async () => {
      db = await SlateDB.open(uniquePath("getstr"), url);
    });

    afterAll(async () => {
      await db.close();
    });

    test("getString on db", async () => {
      await db.put(Buffer.from("gs1"), Buffer.from("hello"), false);
      expect(await db.getString(Buffer.from("gs1"))).toBe("hello");
    });

    test("getString returns null for missing key", async () => {
      expect(await db.getString(Buffer.from("gs_missing"))).toBeNull();
    });

    test("getString on transaction", async () => {
      const txn = await db.begin();
      await txn.put(Buffer.from("gs_txn"), Buffer.from("txn_val"));
      expect(await txn.getString(Buffer.from("gs_txn"))).toBe("txn_val");
      await txn.rollback();
    });
  });

  // =========================================================================
  // read-only mode (requires persistent backend — skipped for :memory:)
  // =========================================================================
  const skipReadOnly = url === ":memory:";
  describe(`[${label}] read-only mode`, () => {
    test.skipIf(skipReadOnly)("reader can read data written by writer", async () => {
      const dbPath = uniquePath("ro");
      const writer = await SlateDB.open(dbPath, url);
      await writer.put(Buffer.from("ro_k1"), Buffer.from("v1"));
      await writer.put(Buffer.from("ro_k2"), Buffer.from("v2"));
      await writer.flush();
      await writer.close();

      const reader = await SlateDB.open(dbPath, url, { readOnly: true });
      expect(await reader.getString(Buffer.from("ro_k1"))).toBe("v1");
      expect(await reader.getString(Buffer.from("ro_k2"))).toBe("v2");
      expect(await reader.get(Buffer.from("ro_missing"))).toBeNull();
      await reader.close();
    });

    test.skipIf(skipReadOnly)("reader scan and scanPrefix work", async () => {
      const dbPath = uniquePath("ro_scan");
      const writer = await SlateDB.open(dbPath, url);
      await writer.put(Buffer.from("rs:a"), Buffer.from("1"));
      await writer.put(Buffer.from("rs:b"), Buffer.from("2"));
      await writer.put(Buffer.from("rs:c"), Buffer.from("3"));
      await writer.flush();
      await writer.close();

      const reader = await SlateDB.open(dbPath, url, { readOnly: true });
      const all = await reader.scan(Buffer.from("rs:"), Buffer.from("rs:~"));
      expect(all).toHaveLength(3);

      const prefixed = await reader.scanPrefix(Buffer.from("rs:"));
      expect(prefixed).toHaveLength(3);
      expect(str(prefixed[0].key)).toBe("rs:a");
      await reader.close();
    });

    test.skipIf(skipReadOnly)("write operations throw in read-only mode", async () => {
      const dbPath = uniquePath("ro_write");
      const writer = await SlateDB.open(dbPath, url);
      await writer.put(Buffer.from("x"), Buffer.from("y"));
      await writer.flush();
      await writer.close();

      const reader = await SlateDB.open(dbPath, url, { readOnly: true });
      try {
        await reader.put(Buffer.from("a"), Buffer.from("b"));
        expect(true).toBe(false); // should not reach
      } catch (e: any) {
        expect(e.message).toContain("read-only");
      }
      await reader.close();
    });
  });
}
