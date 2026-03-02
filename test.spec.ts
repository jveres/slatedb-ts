/**
 * Integration test — faithful port of SlateDB's `examples/src/full_example.rs`.
 *
 * Uses one DB per test group (beforeAll/afterAll) to minimize object store
 * round-trips. This matches how SlateDB's own cloud-compatible test
 * (tests/db.rs) operates — a single DB shared across all operations.
 *
 * All writes use `awaitDurable=false` with explicit `flush()` where needed,
 * matching SlateDB's own pattern. Non-durable writes land in the memtable
 * immediately and are visible within the same process.
 *
 * Usage:
 *   bun test                                              # in-memory only
 *   SLATEDB_TEST_URLS=":memory:,file:///tmp/slate" bun test
 *   SLATEDB_TEST_URLS="s3://my-bucket" bun test
 *
 * @see https://github.com/slatedb/slatedb/blob/main/examples/src/full_example.rs
 * @see https://github.com/slatedb/slatedb/blob/main/slatedb/tests/db.rs
 */
import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { SlateDB, WriteBatch, Transaction, IsolationLevel } from "./index";

// ---------------------------------------------------------------------------
// Backend resolution
// ---------------------------------------------------------------------------
const urls = (process.env.SLATEDB_TEST_URLS ?? ":memory:")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const dec = new TextDecoder();
const str = (b: Uint8Array) => dec.decode(b);

/** Unique path per run so persistent backends don't collide. */
function uniquePath(tag: string): string {
  return `/tmp/slatedb_ts_${tag}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// ---------------------------------------------------------------------------
// Tests — run the full suite once per backend URL
// ---------------------------------------------------------------------------
for (const url of urls) {
  const label = url === ":memory:" ? "memory" : url;

  describe(`[${label}] full_example.rs`, () => {
    let db: SlateDB;

    beforeAll(() => {
      const path = uniquePath("full");
      console.log(`    ⏳ opening ${label} (${path})...`);
      const t = performance.now();
      db = SlateDB.open(path, url);
      console.log(`    ✓ opened in ${(performance.now() - t).toFixed(0)}ms`);
    });

    afterAll(() => {
      console.log(`    ⏳ closing...`);
      const t = performance.now();
      db.close();
      console.log(`    ✓ closed in ${(performance.now() - t).toFixed(0)}ms`);
    });

    test("put then get returns the value", () => {
      db.put("test_key", "test_value", false);
      expect(db.getString("test_key")).toBe("test_value");
    });

    test("delete removes the key", () => {
      db.put("del_key", "del_value", false);
      expect(db.getString("del_key")).toBe("del_value");

      db.delete("del_key", false);
      expect(db.get("del_key")).toBeNull();
    });

    test("scan over unbounded range returns all keys in sorted order", () => {
      db.put("scan_key1", "scan_value1", false);
      db.put("scan_key2", "scan_value2", false);
      db.put("scan_key3", "scan_value3", false);
      db.put("scan_key4", "scan_value4", false);

      const items = db.scan("scan_key1", "scan_key5");
      expect(items).toHaveLength(4);

      for (let i = 0; i < items.length; i++) {
        const n = i + 1;
        expect(str(items[i].key)).toBe(`scan_key${n}`);
        expect(str(items[i].value)).toBe(`scan_value${n}`);
      }
    });

    test("scan over bounded range [key1, key3) returns matching keys", () => {
      const items = db.scan("scan_key1", "scan_key3");
      expect(items).toHaveLength(2);
      expect(str(items[0].key)).toBe("scan_key1");
      expect(str(items[0].value)).toBe("scan_value1");
      expect(str(items[1].key)).toBe("scan_key2");
      expect(str(items[1].value)).toBe("scan_value2");
    });

    test("scan with start bound returns keys from that point", () => {
      const items = db.scan("scan_key4", "scan_key5");
      expect(items).toHaveLength(1);
      expect(str(items[0].key)).toBe("scan_key4");
      expect(str(items[0].value)).toBe("scan_value4");
    });
  });

  describe(`[${label}] write batch`, () => {
    let db: SlateDB;

    beforeAll(() => {
      db = SlateDB.open(uniquePath("batch"), url);
    });

    afterAll(() => {
      db.close();
    });

    test("atomic multi-put via WriteBatch", () => {
      const batch = new WriteBatch();
      batch.put("b1", "v1");
      batch.put("b2", "v2");
      batch.put("b3", "v3");
      db.writeBatch(batch, false);

      expect(db.getString("b1")).toBe("v1");
      expect(db.getString("b2")).toBe("v2");
      expect(db.getString("b3")).toBe("v3");
    });

    test("WriteBatch with deletes", () => {
      db.put("bd1", "before", false);
      db.put("bd2", "keep", false);

      const batch = new WriteBatch();
      batch.delete("bd1");
      batch.put("bd3", "new");
      db.writeBatch(batch, false);

      expect(db.get("bd1")).toBeNull();
      expect(db.getString("bd2")).toBe("keep");
      expect(db.getString("bd3")).toBe("new");
    });

    test("WriteBatch non-durable", () => {
      const batch = new WriteBatch();
      batch.put("bnd1", "fast");
      db.writeBatch(batch, false);

      expect(db.getString("bnd1")).toBe("fast");
    });

    test("free unused WriteBatch without error", () => {
      const batch = new WriteBatch();
      batch.put("unused", "data");
      batch.free(); // should not throw
    });
  });

  describe(`[${label}] transactions`, () => {
    let db: SlateDB;

    beforeAll(() => {
      db = SlateDB.open(uniquePath("txn"), url);
    });

    afterAll(() => {
      db.close();
    });

    test("transaction commit makes writes visible", () => {
      const txn = db.begin();
      txn.put("tk1", "tv1");
      txn.put("tk2", "tv2");
      txn.commit(false);

      expect(db.getString("tk1")).toBe("tv1");
      expect(db.getString("tk2")).toBe("tv2");
    });

    test("transaction rollback discards writes", () => {
      db.put("tr_existing", "original", false);

      const txn = db.begin();
      txn.put("tr_existing", "modified");
      txn.put("tr_new_key", "new_val");
      txn.rollback();

      expect(db.getString("tr_existing")).toBe("original");
      expect(db.get("tr_new_key")).toBeNull();
    });

    test("transaction read-your-writes", () => {
      db.put("ryw", "before", false);

      const txn = db.begin();
      txn.put("ryw", "inside_txn");
      expect(txn.getString("ryw")).toBe("inside_txn");
      txn.commit(false);

      expect(db.getString("ryw")).toBe("inside_txn");
    });

    test("transaction delete", () => {
      db.put("td", "will_delete", false);

      const txn = db.begin();
      txn.delete("td");
      expect(txn.get("td")).toBeNull();
      txn.commit(false);

      expect(db.get("td")).toBeNull();
    });

    test("transaction commit non-durable", () => {
      const txn = db.begin();
      txn.put("nd_txn", "fast");
      txn.commit(false);

      expect(db.getString("nd_txn")).toBe("fast");
    });

    test("transaction with SerializableSnapshot isolation", () => {
      const txn = db.begin(IsolationLevel.SerializableSnapshot);
      txn.put("iso", "serializable");
      txn.commit(false);

      expect(db.getString("iso")).toBe("serializable");
    });
  });

  describe(`[${label}] bridge extras`, () => {
    let db: SlateDB;

    beforeAll(() => {
      db = SlateDB.open(uniquePath("extra"), url);
    });

    afterAll(() => {
      db.close();
    });

    test("binary keys and values round-trip", () => {
      const key = new Uint8Array([0xCA, 0xFE]);
      const val = new Uint8Array([0xBE, 0xEF]);
      db.put(key, val, false);

      const got = db.get(key);
      expect(got).not.toBeNull();
      expect(got![0]).toBe(0xBE);
      expect(got![1]).toBe(0xEF);
    });

    test("get on missing key returns null", () => {
      expect(db.get("no_such_key")).toBeNull();
      expect(db.getString("no_such_key")).toBeNull();
    });

    test("put empty value", () => {
      db.put("empty", new Uint8Array(0), false);
      const got = db.get("empty");
      expect(got).not.toBeNull();
      expect(got!.byteLength).toBe(0);
    });

    test("overwrite replaces value", () => {
      db.put("ow_k", "v1", false);
      expect(db.getString("ow_k")).toBe("v1");
      db.put("ow_k", "v2", false);
      expect(db.getString("ow_k")).toBe("v2");
    });

    test("delete non-existent key is a no-op", () => {
      db.delete("ghost", false);
    });

    test("scan empty prefix returns empty array", () => {
      expect(db.scan("zzz_no_match", "zzz_no_match~")).toEqual([]);
    });

    test("flush does not lose data", () => {
      db.put("fl1", "v1", false);
      db.flush();
      expect(db.getString("fl1")).toBe("v1");
    });

    test("many sequential writes maintain scan order", () => {
      const n = 100;
      for (let i = 0; i < n; i++) {
        const key = `seq_${String(i).padStart(4, "0")}`;
        db.put(key, `val_${i}`, false);
      }
      db.flush();

      const items = db.scan("seq_", "seq_~");
      expect(items).toHaveLength(n);

      for (let i = 1; i < items.length; i++) {
        const prev = str(items[i - 1].key);
        const curr = str(items[i].key);
        expect(prev < curr).toBe(true);
      }
    });
  });
}
