import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Store } from "../index";

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "timslite-test-"));
}

function withStore(fn: (store: Store) => void): void {
  const dir = makeTmpDir();
  try {
    const store = Store.open(dir, { enableBackgroundThread: false });
    fn(store);
    store.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("dataset", () => {
  describe("write and read", () => {
    it("single write and read", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("hello"));
        const result = ds.read(1n);
        assert(result !== null);
        const [ts, data] = result;
        assert.equal(ts, 1n);
        assert.deepEqual(Buffer.from(data), Buffer.from("hello"));
        ds.close();
      });
    });

    it("read nonexistent returns null", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        assert.equal(ds.read(999n), null);
        ds.close();
      });
    });

    it("multiple writes and reads", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        for (let i = 1n; i <= 100n; i++) {
          ds.write(i, Buffer.from(`record_${i}`));
        }
        for (let i = 1n; i <= 100n; i++) {
          const result = ds.read(i);
          assert(result !== null);
          const [ts, data] = result;
          assert.equal(ts, i);
          assert.deepEqual(Buffer.from(data), Buffer.from(`record_${i}`));
        }
        ds.close();
      });
    });

    it("write with Uint8Array payload", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        const payload = new Uint8Array([0x00, 0x01, 0x02, 0xff]);
        ds.write(1n, payload);
        const result = ds.read(1n);
        assert(result !== null);
        assert.deepEqual(new Uint8Array(result[1]), payload);
        ds.close();
      });
    });

    it("write empty buffer", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.alloc(0));
        const result = ds.read(1n);
        assert(result !== null);
        assert.equal(result[1].length, 0);
        ds.close();
      });
    });
  });

  describe("readLatest", () => {
    it("returns latest written record", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("first"));
        ds.write(5n, Buffer.from("last"));
        const result = ds.readLatest();
        assert(result !== null);
        assert.equal(result[0], 5n);
        assert.deepEqual(Buffer.from(result[1]), Buffer.from("last"));
        ds.close();
      });
    });

    it("returns null for empty dataset", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        assert.equal(ds.readLatest(), null);
        ds.close();
      });
    });

    it("latestTimestamp property", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        assert.equal(ds.latestTimestamp, null);
        ds.write(10n, Buffer.from("data"));
        assert.equal(ds.latestTimestamp, 10n);
        ds.write(20n, Buffer.from("more"));
        assert.equal(ds.latestTimestamp, 20n);
        ds.close();
      });
    });
  });

  describe("append", () => {
    it("append creates new record", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.append(1n, Buffer.from("hello"));
        const result = ds.read(1n);
        assert(result !== null);
        assert.deepEqual(Buffer.from(result[1]), Buffer.from("hello"));
        ds.close();
      });
    });

    it("append to latest record concatenates", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.append(1n, Buffer.from("abc"));
        ds.append(1n, Buffer.from("de"));
        const result = ds.read(1n);
        assert(result !== null);
        assert.deepEqual(Buffer.from(result[1]), Buffer.from("abcde"));
        ds.close();
      });
    });

    it("append with empty buffer is no-op on existing", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.append(1n, Buffer.from("abc"));
        ds.append(1n, Buffer.alloc(0));
        const result = ds.read(1n);
        assert(result !== null);
        assert.deepEqual(Buffer.from(result[1]), Buffer.from("abc"));
        ds.close();
      });
    });

    it("append forward creates new timestamp", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.append(1n, Buffer.from("first"));
        ds.append(2n, Buffer.from("second"));
        const r1 = ds.read(1n);
        assert(r1 !== null);
        assert.deepEqual(Buffer.from(r1[1]), Buffer.from("first"));
        const r2 = ds.read(2n);
        assert(r2 !== null);
        assert.deepEqual(Buffer.from(r2[1]), Buffer.from("second"));
        ds.close();
      });
    });

    it("append with timestamp before latest throws", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(10n, Buffer.from("data"));
        assert.throws(() => ds.append(5n, Buffer.from("old")));
        ds.close();
      });
    });
  });

  describe("delete", () => {
    it("delete removes record", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("temp"));
        const r1 = ds.read(1n);
        assert(r1 !== null);
        assert.deepEqual(Buffer.from(r1[1]), Buffer.from("temp"));
        ds.delete(1n);
        assert.equal(ds.read(1n), null);
        ds.close();
      });
    });

    it("delete nonexistent on non-empty dataset throws", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("data"));
        assert.throws(() => ds.delete(999n));
        ds.close();
      });
    });
  });

  describe("query", () => {
    it("query returns records in range", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        for (let i = 1n; i <= 10n; i++) {
          ds.write(i, Buffer.from(`r${i}`));
        }
        const results: [number, Buffer][] = [];
        for (const [ts, data] of ds.query(3n, 7n)) {
          results.push([ts, Buffer.from(data)]);
        }
        assert.equal(results.length, 5);
        assert.equal(results[0][0], 3);
        assert.equal(results[4][0], 7);
        ds.close();
      });
    });

    it("queryAll returns all results as array", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        for (let i = 1n; i <= 5n; i++) {
          ds.write(i, Buffer.from(`r${i}`));
        }
        const results = ds.queryAll(1n, 5n);
        assert.equal(results.length, 5);
        assert.equal(results[0][0], 1n);
        assert.equal(results[4][0], 5n);
        ds.close();
      });
    });

    it("query empty range returns no results", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(100n, Buffer.from("data"));
        const results = ds.queryAll(1n, 50n);
        assert.equal(results.length, 0);
        ds.close();
      });
    });

    it("queryExist returns bitmap buffer", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("data"));
        const bitmap = ds.queryExist(1n, 1n);
        assert(Buffer.isBuffer(bitmap));
        assert(bitmap.length > 0);
        ds.close();
      });
    });

    it("queryLength returns iterator of (timestamp, length)", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        for (let i = 1n; i <= 10n; i++) {
          ds.write(i, Buffer.from(`r${i}`));
        }
        const results: [number, number][] = [];
        for (const [ts, len] of ds.queryLength(3n, 7n)) {
          results.push([ts, len]);
        }
        assert.equal(results.length, 5);
        assert.equal(results[0][0], 3);
        assert.equal(results[4][0], 7);
        for (const [, len] of results) {
          assert(typeof len === "number");
          assert(len > 0);
        }
        ds.close();
      });
    });

    it("queryLengthAll returns array of (timestamp, length)", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        for (let i = 1n; i <= 20n; i++) {
          ds.write(i, Buffer.from(`r${i}`));
        }
        const results = ds.queryLengthAll(1n, 20n);
        assert.equal(results.length, 20);
        for (const [ts, len] of results) {
          assert(typeof ts === "bigint");
          assert(typeof len === "number");
          assert(len > 0);
        }
        ds.close();
      });
    });

    it("queryAll matches list(query())", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        for (let i = 1n; i <= 10n; i++) {
          ds.write(i, Buffer.from(`d${i}`));
        }
        const arrayResult = ds.queryAll(1n, 10n);
        const iterResult: [bigint, Buffer][] = [];
        for (const [ts, data] of ds.query(1n, 10n)) {
          iterResult.push([BigInt(ts), Buffer.from(data)]);
        }
        assert.equal(arrayResult.length, iterResult.length);
        for (let i = 0; i < arrayResult.length; i++) {
          assert.equal(arrayResult[i][0], iterResult[i][0]);
          assert.deepEqual(arrayResult[i][1], iterResult[i][1]);
        }
        ds.close();
      });
    });
  });

  describe("flush and inspect", () => {
    it("flush does not throw", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("data"));
        ds.flush();
        ds.close();
      });
    });

    it("inspect returns info and state", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("data"));
        const result = ds.inspect();
        assert.equal(result.info.name, "test");
        assert.equal(result.info.datasetType, "data");
        assert(typeof result.info.identifier === "bigint");
        assert(typeof result.state.readOnly === "boolean");
        assert(typeof result.state.hasBlockCache === "boolean");
        assert(typeof result.state.hasJournal === "boolean");
        assert(typeof result.state.hasQueue === "boolean");
        ds.close();
      });
    });

    it("identifier is stable", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        const id = ds.identifier;
        assert(typeof id === "bigint");
        assert(id > 0n);
        ds.close();

        const ds2 = store.openDataset("test", "data");
        assert.equal(ds2.identifier, id);
        ds2.close();
      });
    });

    it("id getter returns handle id", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        const hid = ds.id;
        assert(typeof hid === "bigint");
        ds.close();
      });
    });

    it("dataDir getter returns path", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        const dir = ds.dataDir;
        assert(typeof dir === "string");
        assert(dir.length > 0);
        ds.close();
      });
    });
  });

  describe("closed dataset operations", () => {
    it("operations on closed dataset throw", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const ds = store.openDataset("test", "data");
        ds.close();
        assert.throws(() => ds.write(1n, Buffer.from("data")));
        assert.throws(() => ds.read(1n));
        assert.throws(() => ds.readLatest());
        assert.throws(() => ds.append(1n, Buffer.from("data")));
        assert.throws(() => ds.delete(1n));
        assert.throws(() => ds.flush());
        assert.throws(() => ds.inspect());
      });
    });
  });
});
