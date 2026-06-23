import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Store, Dataset, version } from "../index";

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "timslite-test-"));
}

describe("basic", () => {
  it("version() returns a semver string", () => {
    assert.equal(typeof version(), "string");
    assert.match(version(), /^\d+\.\d+\.\d+$/);
  });

  it("Store.open and Store.close", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      assert(store instanceof Store);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("Store.open with config", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      assert(store instanceof Store);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("close twice throws", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.close();
      assert.throws(() => store.close());
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("operations on closed store throw", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.close();
      assert.throws(() => store.createDataset("test", "data"));
      assert.throws(() => store.openDataset("test", "data"));
      assert.throws(() => store.dropDataset("test", "data"));
      assert.throws(() => store.getDatasetNames());
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("createDataset and openDataset lifecycle", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("sensor", "waveform");
      const ds = store.openDataset("sensor", "waveform");
      assert(ds instanceof Dataset);
      ds.close();
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("createDataset duplicate throws", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("sensor", "waveform");
      assert.throws(() => store.createDataset("sensor", "waveform"));
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("openDataset nonexistent throws", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      assert.throws(() => store.openDataset("nonexistent", "data"));
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("dropDataset then recreate", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("temp", "data");
      const ds = store.openDataset("temp", "data");
      ds.write(1n, Buffer.from("hello"));
      ds.close();
      store.dropDataset("temp", "data");
      store.createDataset("temp", "data");
      const ds2 = store.openDataset("temp", "data");
      assert.equal(ds2.read(1n), null);
      ds2.close();
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("getDatasetNames returns created datasets", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("alpha", "data");
      store.createDataset("beta", "events");
      const names = store.getDatasetNames().sort();
      assert.deepEqual(names, ["alpha", "beta"]);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("getDatasetTypes returns types for a dataset name", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("sensor", "waveform");
      store.createDataset("sensor", "events");
      const types = store.getDatasetTypes("sensor").sort();
      assert.deepEqual(types, ["events", "waveform"]);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("openDatasetByIdentifier", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("test", "data");
      const ds = store.openDataset("test", "data");
      const id = ds.identifier;
      assert(typeof id === "bigint");
      ds.close();

      const ds2 = store.openDatasetByIdentifier(id);
      assert(ds2 instanceof Dataset);
      ds2.write(1n, Buffer.from("hello"));
      const result = ds2.read(1n);
      assert(result !== null);
      assert.deepEqual(Buffer.from(result[1]), Buffer.from("hello"));
      ds2.close();
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("inspectDataset returns info and state", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      store.createDataset("test", "data");
      const result = store.inspectDataset("test", "data");
      assert.equal(result.info.name, "test");
      assert.equal(result.info.datasetType, "data");
      assert(typeof result.info.identifier === "bigint");
      assert(typeof result.state.readOnly === "boolean");
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("background tasks tick", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir, { enableBackgroundThread: false });
      const result = store.tickBackgroundTasks();
      assert(typeof result.executedTasks === "number");
      assert(typeof result.nextDelayMs === "number");
      const delay = store.nextBackgroundDelay();
      assert(typeof delay === "number");
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("closed getter", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      assert.equal(store.closed, false);
      store.close();
      assert.equal(store.closed, true);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("readOnly getter", () => {
    const dir = makeTmpDir();
    try {
      const store = Store.open(dir);
      assert.equal(store.readOnly, false);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
