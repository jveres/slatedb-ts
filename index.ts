import { dlopen, FFIType, ptr, suffix } from "bun:ffi";
import { resolve } from "path";

// ---------------------------------------------------------------------------
// Load shared library
// ---------------------------------------------------------------------------
const LIB = resolve(import.meta.dir, `target/release/libslatedb_ffi.${suffix}`);

const { symbols: ffi } = dlopen(LIB, {
  // lifecycle
  slatedb_open:       { args: [FFIType.ptr, FFIType.ptr],                                       returns: FFIType.ptr },
  slatedb_close:      { args: [FFIType.ptr],                                                    returns: FFIType.void },
  // kv ops
  slatedb_put:        { args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64, FFIType.i32], returns: FFIType.i32 },
  slatedb_get:        { args: [FFIType.ptr, FFIType.ptr, FFIType.u64],                          returns: FFIType.ptr },
  slatedb_delete:     { args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.i32],              returns: FFIType.i32 },
  slatedb_flush:      { args: [FFIType.ptr],                                                    returns: FFIType.i32 },
  // scan
  slatedb_scan:       { args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64], returns: FFIType.ptr },
  // buf accessors
  slatedb_buf_len:    { args: [FFIType.ptr], returns: FFIType.u32 },
  slatedb_buf_copy:   { args: [FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.u64 },
  slatedb_buf_free:   { args: [FFIType.ptr], returns: FFIType.void },
  // scan accessors
  slatedb_scan_count: { args: [FFIType.ptr], returns: FFIType.u32 },
  slatedb_scan_len:   { args: [FFIType.ptr], returns: FFIType.u32 },
  slatedb_scan_copy:  { args: [FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.u64 },
  slatedb_scan_free:  { args: [FFIType.ptr], returns: FFIType.void },
  // write batch
  slatedb_batch_new:    { args: [],                                                               returns: FFIType.ptr },
  slatedb_batch_put:    { args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64], returns: FFIType.void },
  slatedb_batch_delete: { args: [FFIType.ptr, FFIType.ptr, FFIType.u64],                          returns: FFIType.void },
  slatedb_batch_write:  { args: [FFIType.ptr, FFIType.ptr, FFIType.i32],                          returns: FFIType.i32 },
  slatedb_batch_free:   { args: [FFIType.ptr],                                                    returns: FFIType.void },
  // transactions
  slatedb_txn_begin:    { args: [FFIType.ptr, FFIType.i32],                                       returns: FFIType.ptr },
  slatedb_txn_put:      { args: [FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.ptr, FFIType.u64], returns: FFIType.i32 },
  slatedb_txn_get:      { args: [FFIType.ptr, FFIType.ptr, FFIType.u64],                          returns: FFIType.ptr },
  slatedb_txn_delete:   { args: [FFIType.ptr, FFIType.ptr, FFIType.u64],                          returns: FFIType.i32 },
  slatedb_txn_commit:   { args: [FFIType.ptr, FFIType.i32],                                       returns: FFIType.i32 },
  slatedb_txn_rollback: { args: [FFIType.ptr],                                                    returns: FFIType.void },
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const enc = new TextEncoder();
const dec = new TextDecoder();

function encode(v: string | Uint8Array): Uint8Array {
  return typeof v === "string" ? enc.encode(v) : v;
}

/** Null-terminated C string. */
function cstr(s: string): Uint8Array {
  return enc.encode(s + "\0");
}

/** Read a little-endian u32 from a Uint8Array at offset. */
function readU32LE(buf: Uint8Array, off: number): number {
  return buf[off] | (buf[off + 1] << 8) | (buf[off + 2] << 16) | (buf[off + 3] << 24) >>> 0;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------
export interface KeyValue {
  key: Uint8Array;
  value: Uint8Array;
}

export class SlateDB {
  #h: number;

  private constructor(h: number) { this.#h = h; }

  /**
   * Open a SlateDB instance.
   * @param path  Logical prefix inside the object store
   * @param url   `":memory:"`, `"s3://bucket"`, etc.
   */
  static open(path: string, url = ":memory:"): SlateDB {
    const p = cstr(path), u = cstr(url);
    const h = ffi.slatedb_open(ptr(p), ptr(u));
    if (!h) throw new Error(`Failed to open SlateDB ("${path}", "${url}")`);
    return new SlateDB(h as number);
  }

  /**
   * Put a key-value pair.
   * @param awaitDurable  Wait for persistence to object storage (default: true).
   *                      Pass `false` for lower latency fire-and-forget writes.
   */
  put(key: string | Uint8Array, value: string | Uint8Array, awaitDurable = true): void {
    const k = encode(key), v = encode(value);
    // Bun FFI rejects ptr() on zero-length buffers, pass null+0 instead
    const vPtr = v.byteLength > 0 ? ptr(v) : null;
    if (ffi.slatedb_put(this.#h, ptr(k), k.byteLength, vPtr, v.byteLength, awaitDurable ? 1 : 0) !== 0)
      throw new Error("put failed");
  }

  /** Get raw bytes by key. Returns `null` if not found. */
  get(key: string | Uint8Array): Uint8Array | null {
    const k = encode(key);
    const buf = ffi.slatedb_get(this.#h, ptr(k), k.byteLength);
    if (!buf) return null;
    const len = Number(ffi.slatedb_buf_len(buf));
    const out = new Uint8Array(len);             // JS-owned memory
    if (len > 0) ffi.slatedb_buf_copy(buf, ptr(out), len);  // Rust copies into it
    ffi.slatedb_buf_free(buf);
    return out;
  }

  /** Get value as a UTF-8 string. Returns `null` if not found. */
  getString(key: string | Uint8Array): string | null {
    const v = this.get(key);
    return v ? dec.decode(v) : null;
  }

  /** Delete a key. */
  delete(key: string | Uint8Array, awaitDurable = true): void {
    const k = encode(key);
    if (ffi.slatedb_delete(this.#h, ptr(k), k.byteLength, awaitDurable ? 1 : 0) !== 0)
      throw new Error("delete failed");
  }

  /** Flush the memtable to object storage. */
  flush(): void {
    if (ffi.slatedb_flush(this.#h) !== 0) throw new Error("flush failed");
  }

  /**
   * Range scan `[start, end)`. Both bounds optional (full scan if omitted).
   */
  scan(start?: string | Uint8Array, end?: string | Uint8Array): KeyValue[] {
    const s = start != null ? encode(start) : null;
    const e = end   != null ? encode(end)   : null;

    const res = ffi.slatedb_scan(
      this.#h,
      s ? ptr(s) : null, s ? s.byteLength : 0,
      e ? ptr(e) : null, e ? e.byteLength : 0,
    );
    if (!res) return [];

    const count = Number(ffi.slatedb_scan_count(res));
    const len   = Number(ffi.slatedb_scan_len(res));
    const raw   = new Uint8Array(len);             // JS-owned memory
    if (len > 0) ffi.slatedb_scan_copy(res, ptr(raw), len);  // Rust copies into it
    ffi.slatedb_scan_free(res);

    // Decode flat buffer: [key_len:u32_le, key, val_len:u32_le, val]*
    // Use subarray() — zero-copy views into `raw` — instead of slice()
    const out: KeyValue[] = new Array(count);
    let off = 0;
    for (let i = 0; i < count; i++) {
      const kl = readU32LE(raw, off); off += 4;
      const key = raw.subarray(off, off + kl); off += kl;
      const vl = readU32LE(raw, off); off += 4;
      const value = raw.subarray(off, off + vl); off += vl;
      out[i] = { key, value };
    }

    return out;
  }

  /**
   * Write a batch atomically.
   * @param awaitDurable  Wait for persistence (default: true).
   */
  writeBatch(batch: WriteBatch, awaitDurable = true): void {
    const bh = batch._take();
    if (!bh) throw new Error("WriteBatch already consumed");
    if (ffi.slatedb_batch_write(this.#h, bh, awaitDurable ? 1 : 0) !== 0)
      throw new Error("writeBatch failed");
  }

  /**
   * Begin an ACID transaction.
   * @param isolation  Isolation level (default: Snapshot).
   */
  begin(isolation: IsolationLevel = IsolationLevel.Snapshot): Transaction {
    const th = ffi.slatedb_txn_begin(this.#h, isolation as number);
    if (!th) throw new Error("begin transaction failed");
    return new Transaction(th as number);
  }

  /** Close the database and free native resources. */
  close(): void {
    if (this.#h) { ffi.slatedb_close(this.#h); this.#h = 0; }
  }
}

// ---------------------------------------------------------------------------
// WriteBatch
// ---------------------------------------------------------------------------

/** Batch multiple writes into a single atomic operation. */
export class WriteBatch {
  #h: number;

  constructor() {
    this.#h = ffi.slatedb_batch_new() as number;
    if (!this.#h) throw new Error("Failed to create WriteBatch");
  }

  /** Add a put to the batch. */
  put(key: string | Uint8Array, value: string | Uint8Array): this {
    const k = encode(key), v = encode(value);
    const vPtr = v.byteLength > 0 ? ptr(v) : null;
    ffi.slatedb_batch_put(this.#h, ptr(k), k.byteLength, vPtr, v.byteLength);
    return this;
  }

  /** Add a delete to the batch. */
  delete(key: string | Uint8Array): this {
    const k = encode(key);
    ffi.slatedb_batch_delete(this.#h, ptr(k), k.byteLength);
    return this;
  }

  /** @internal — return handle and detach ownership (consumed by write). */
  _take(): number {
    const h = this.#h;
    this.#h = 0;
    return h;
  }

  /** Free the batch without writing. */
  free(): void {
    if (this.#h) { ffi.slatedb_batch_free(this.#h); this.#h = 0; }
  }
}

// ---------------------------------------------------------------------------
// Transaction isolation levels
// ---------------------------------------------------------------------------

export enum IsolationLevel {
  /** Snapshot — detects write-write conflicts only. */
  Snapshot = 0,
  /** Serializable Snapshot — detects read-write and write-write conflicts. */
  SerializableSnapshot = 1,
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/** ACID transaction on SlateDB. */
export class Transaction {
  #h: number;

  /** @internal */
  constructor(h: number) { this.#h = h; }

  /** Put within the transaction. */
  put(key: string | Uint8Array, value: string | Uint8Array): void {
    const k = encode(key), v = encode(value);
    const vPtr = v.byteLength > 0 ? ptr(v) : null;
    if (ffi.slatedb_txn_put(this.#h, ptr(k), k.byteLength, vPtr, v.byteLength) !== 0)
      throw new Error("transaction put failed");
  }

  /** Get within the transaction. Returns `null` if not found. */
  get(key: string | Uint8Array): Uint8Array | null {
    const k = encode(key);
    const buf = ffi.slatedb_txn_get(this.#h, ptr(k), k.byteLength);
    if (!buf) return null;
    const len = Number(ffi.slatedb_buf_len(buf));
    const out = new Uint8Array(len);
    if (len > 0) ffi.slatedb_buf_copy(buf, ptr(out), len);
    ffi.slatedb_buf_free(buf);
    return out;
  }

  /** Get within the transaction as UTF-8 string. */
  getString(key: string | Uint8Array): string | null {
    const v = this.get(key);
    return v ? dec.decode(v) : null;
  }

  /** Delete within the transaction. */
  delete(key: string | Uint8Array): void {
    const k = encode(key);
    if (ffi.slatedb_txn_delete(this.#h, ptr(k), k.byteLength) !== 0)
      throw new Error("transaction delete failed");
  }

  /**
   * Commit the transaction.
   * @param awaitDurable  Wait for persistence (default: true).
   * @throws on conflict or closed DB.
   */
  commit(awaitDurable = true): void {
    const h = this.#h; this.#h = 0;
    if (ffi.slatedb_txn_commit(h, awaitDurable ? 1 : 0) !== 0)
      throw new Error("transaction commit failed (conflict?)");
  }

  /** Rollback (abort) the transaction. */
  rollback(): void {
    if (this.#h) { ffi.slatedb_txn_rollback(this.#h); this.#h = 0; }
  }
}

export default SlateDB;
