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

describe("journal", () => {
  describe("read and query", () => {
    it("journalRead returns sequence and payload", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const latest = store.journalLatestSequence();
        assert(latest !== null);
        assert(typeof latest === "bigint");
        assert(latest >= 1n);

        const first = store.journalRead(1n);
        assert(first !== null);
        assert.equal(first[0], 1n);
        assert(first[1].length > 0);
      });
    });

    it("journalQuery returns range", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const latest = store.journalLatestSequence();
        assert(latest !== null);

        const rows = store.journalQuery(1n, latest);
        assert(rows.length > 0);
        assert.equal(rows[0][0], 1n);
        assert(rows[0][1].length > 0);
      });
    });

    it("journalRead nonexistent returns null", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const latest = store.journalLatestSequence();
        assert(latest !== null);
        assert.equal(store.journalRead(latest + 100n), null);
      });
    });

    it("journalLatestSequence grows after writes", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const before = store.journalLatestSequence();
        assert(before !== null);

        const ds = store.openDataset("test", "data");
        ds.write(1n, Buffer.from("payload"));
        ds.close();

        const after = store.journalLatestSequence();
        assert(after !== null);
        assert(after > before);
      });
    });
  });

  describe("journal queue", () => {
    it("journalQueue push/poll/ack", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const beforeLatest = store.journalLatestSequence();
        assert(beforeLatest !== null);

        const jq = store.openJournalQueue();
        const jc = jq.openConsumer("journal-worker");

        const ds = store.openDataset("test", "data");
        ds.write(10n, Buffer.from("payload"));
        ds.close();

        const result = jc.pollSync(200);
        assert(result !== null);
        assert(result[0] > beforeLatest);
        assert(result[1].length > 0);
        jc.ack(result[0]);

        jq.close();
      });
    });

    it("journalQueue async poll", async () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableBackgroundThread: false });
        store.createDataset("test", "data");
        const beforeLatest = store.journalLatestSequence();
        assert(beforeLatest !== null);

        const jq = store.openJournalQueue();
        const jc = jq.openConsumer("journal-worker");

        const ds = store.openDataset("test", "data");
        ds.write(10n, Buffer.from("async-payload"));
        ds.close();

        const result = await jc.poll(300);
        assert(result !== null);
        assert(result[0] > beforeLatest);
        jc.ack(result[0]);

        jq.close();
        store.close();
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });

    it("journalQueue pollCallback", async () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableBackgroundThread: false });
        store.createDataset("test", "data");
        const jq = store.openJournalQueue();
        const jc = jq.openConsumer("journal-worker");

        let called = false;
        jc.pollCallback(() => {
          called = true;
        });

        const ds = store.openDataset("test", "data");
        ds.write(10n, Buffer.from("data"));
        ds.close();

        await new Promise((resolve) => setTimeout(resolve, 50));
        jc.pollSync(200);
        assert.equal(called, true);

        jq.close();
        store.close();
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });

    it("closed journal queue operations throw", () => {
      withStore((store) => {
        store.createDataset("test", "data");
        const jq = store.openJournalQueue();
        jq.close();
        assert.throws(() => jq.openConsumer("worker"));
      });
    });
  });

  describe("disabled journal", () => {
    it("journal API throws when journal disabled", () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableJournal: false, enableBackgroundThread: false });
        store.createDataset("test", "data");

        assert.throws(() => store.journalRead(1n));
        assert.throws(() => store.journalQuery(1n, 10n));
        assert.throws(() => store.journalLatestSequence());
        assert.throws(() => store.openJournalQueue());

        store.close();
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });
  });
});
