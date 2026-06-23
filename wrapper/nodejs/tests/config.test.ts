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
        enableJournal: true,
      });
      const ds = store.openDataset("custom", "events");
      const result = store.inspectDataset("custom", "events");
      assert.equal(result.info.compressLevel, 6);
      assert.equal(result.info.indexContinuous, 1);
      assert.equal(result.info.retentionWindow, 86400000n);
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
});
