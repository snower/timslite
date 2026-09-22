import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Store } from "../index";

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "timslite-test-"));
}

describe("config", () => {
  it("StoreConfig empty object uses defaults", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, {});
      assert(store instanceof Store);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("StoreConfig with all fields", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, {
        flushIntervalMs: 5000,
        idleTimeoutMs: 60000,
        dataSegmentSize: BigInt(64 * 1024 * 1024),
        indexSegmentSize: BigInt(4 * 1024 * 1024),
        initialDataSegmentSize: BigInt(256 * 1024),
        initialIndexSegmentSize: BigInt(4096),
        compressLevel: 9,
        compressType: 0,
        cacheMaxMemory: BigInt(128 * 1024 * 1024),
        cacheIdleTimeoutMs: 600,
        retentionCheckHour: 3,
        enableBackgroundThread: false,
        enableJournal: false,
      });
      assert(store instanceof Store);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("StoreConfig readOnly mode rejects writes", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("test", "data");
      const ds = store.openDataset("test", "data");
      ds.write(1n, Buffer.from("hello"));
      ds.close();
      store.close();

      const store2 = Store.open(dir, { readOnly: true, enableBackgroundThread: false });
      const ds2 = store2.openDataset("test", "data");
      const result = ds2.read(1n);
      assert(result !== null);
      assert.deepEqual(Buffer.from(result[1]), Buffer.from("hello"));
      assert.throws(() => ds2.write(2n, Buffer.from("world")));
      ds2.close();
      store2.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions with all fields", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("custom", "events", {
        dataSegmentSize: BigInt(32 * 1024 * 1024),
        indexSegmentSize: BigInt(2 * 1024 * 1024),
        initialDataSegmentSize: BigInt(128 * 1024),
        initialIndexSegmentSize: BigInt(2048),
        compressLevel: 6,
        compressType: 0,
        indexContinuous: true,
        retentionWindow: 86400000n,
        timestampUnitsPerSeconds: 1000000n,
        enableJournal: true,
      });
      const ds = store.openDataset("custom", "events");
      const result = store.inspectDataset("custom", "events");
      assert.equal(result.info.compressLevel, 6);
      assert.equal(result.info.indexContinuous, 1);
      assert.equal(result.info.retentionWindow, 86400000n);
      assert.equal(result.info.timestampUnitsPerSeconds, 1000000n);
      ds.close();
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions retentionWindow as bigint", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("retention", "data", {
        retentionWindow: BigInt(3600000),
      });
      const result = store.inspectDataset("retention", "data");
      assert.equal(result.info.retentionWindow, 3600000n);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions timestampUnitsPerSeconds as bigint", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("scale", "data", {
        timestampUnitsPerSeconds: BigInt(1000000),
      });
      const result = store.inspectDataset("scale", "data");
      assert.equal(result.info.timestampUnitsPerSeconds, 1000000n);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions u64 fields accept plain numbers", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("requestDetails", "raw", {
        retentionWindow: 100000,
        indexContinuous: false,
        enableJournal: false,
        timestampUnitsPerSeconds: 100,
      });
      const result = store.inspectDataset("requestDetails", "raw");
      assert.equal(result.info.retentionWindow, 100000n);
      assert.equal(result.info.timestampUnitsPerSeconds, 100n);
      assert.equal(result.info.indexContinuous, 0);
      assert.equal(result.info.enableJournal, false);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions segment sizes accept plain numbers", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("num-sizes", "data", {
        dataSegmentSize: 32 * 1024 * 1024,
        indexSegmentSize: 2 * 1024 * 1024,
        initialDataSegmentSize: 128 * 1024,
        initialIndexSegmentSize: 2048,
      });
      const result = store.inspectDataset("num-sizes", "data");
      assert.equal(result.info.dataSegmentSize, BigInt(32 * 1024 * 1024));
      assert.equal(result.info.indexSegmentSize, BigInt(2 * 1024 * 1024));
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("StoreConfig u64 fields accept plain numbers", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, {
        dataSegmentSize: 64 * 1024 * 1024,
        indexSegmentSize: 4 * 1024 * 1024,
        initialDataSegmentSize: 256 * 1024,
        initialIndexSegmentSize: 4096,
        cacheMaxMemory: 128 * 1024 * 1024,
        enableBackgroundThread: false,
      });
      assert(store instanceof Store);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions timestampUnitsPerSeconds defaults to 0", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("legacy", "data", {});
      const result = store.inspectDataset("legacy", "data");
      assert.equal(result.info.timestampUnitsPerSeconds, 0n);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions segment sizes as bigint", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("bigint", "data", {
        dataSegmentSize: BigInt(32 * 1024 * 1024),
        indexSegmentSize: BigInt(2 * 1024 * 1024),
      });
      const result = store.inspectDataset("bigint", "data");
      assert.equal(typeof result.info.dataSegmentSize, "bigint");
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  // Safe-integer contract: a `number` for a u64 option must be a non-negative
  // JS safe integer. `2 ** 64` is the extreme case the old `<= u64::MAX as f64`
  // guard let through, since u64::MAX as f64 rounds up to exactly 2**64 and the
  // value then silently cast to u64::MAX.
  it("CreateDatasetOptions rejects 2**64 given as a plain number", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      // timestampUnitsPerSeconds is a pure u64 field with no narrower core
      // bound, so a rejection here can only come from the number conversion.
      // (retentionWindow would also reject 2**64 via its i64::MAX core cap,
      // masking the wrapper bug.)
      assert.throws(() =>
        store.createDataset("num-2pow64-units", "data", {
          timestampUnitsPerSeconds: 2 ** 64,
        }),
      );
      assert.throws(() =>
        store.createDataset("num-2pow64-seg", "data", {
          dataSegmentSize: 2 ** 64,
        }),
      );
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions rejects numbers above Number.MAX_SAFE_INTEGER", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      assert.equal(Number.isSafeInteger(2 ** 54), false);
      assert.throws(() =>
        store.createDataset("unsafe-retention", "data", {
          retentionWindow: 2 ** 54,
        }),
      );
      assert.throws(() =>
        store.createDataset("unsafe-units", "data", {
          timestampUnitsPerSeconds: 2 ** 54,
        }),
      );
      assert.throws(() =>
        store.createDataset("unsafe-segsize", "data", {
          dataSegmentSize: 2 ** 54,
        }),
      );
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions still accepts plain numbers up to Number.MAX_SAFE_INTEGER", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      store.createDataset("safe-max-num", "data", {
        timestampUnitsPerSeconds: Number.MAX_SAFE_INTEGER,
      });
      const result = store.inspectDataset("safe-max-num", "data");
      assert.equal(
        result.info.timestampUnitsPerSeconds,
        BigInt(Number.MAX_SAFE_INTEGER),
      );
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("CreateDatasetOptions accepts BigInt beyond safe-integer range within u64", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      // BigInt stays lossless above Number.MAX_SAFE_INTEGER across the u64 range.
      store.createDataset("bigint-above-safe", "data", {
        timestampUnitsPerSeconds: 2n ** 54n,
      });
      assert.equal(
        store.inspectDataset("bigint-above-safe", "data").info
          .timestampUnitsPerSeconds,
        2n ** 54n,
      );
      store.createDataset("bigint-u64-max", "data", {
        timestampUnitsPerSeconds: 18446744073709551615n,
      });
      assert.equal(
        store.inspectDataset("bigint-u64-max", "data").info
          .timestampUnitsPerSeconds,
        18446744073709551615n,
      );
      // Past u64 there is no lossless representation, so it stays rejected.
      assert.throws(() =>
        store.createDataset("bigint-overflow", "data", {
          timestampUnitsPerSeconds: 2n ** 64n,
        }),
      );
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
