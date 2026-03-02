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
import { SlateDB, WriteBatch, IsolationLevel } from "./index";

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
}
