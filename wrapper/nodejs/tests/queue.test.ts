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

describe("queue", () => {
  describe("basic push/poll/ack", () => {
    it("push returns timestamp, poll returns record", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker-1");

        const ts = q.push(Buffer.from("hello"));
        assert(typeof ts === "bigint");
        assert(ts > 0n);

        const result = c.pollSync(100);
        assert(result !== null);
        assert.equal(result[0], ts);
        assert.deepEqual(Buffer.from(result[1]), Buffer.from("hello"));

        c.ack(ts);
        assert.equal(c.pollSync(50), null);

        q.close();
        ds.close();
      });
    });

    it("multiple pushes sequential poll", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker-1");

        for (let i = 0; i < 10; i++) {
          const ts = q.push(Buffer.from(`msg_${i}`));
          assert.equal(ts, BigInt(i + 1));
        }

        for (let i = 0; i < 10; i++) {
          const result = c.pollSync(50);
          assert(result !== null);
          assert.equal(result[0], BigInt(i + 1));
          assert.deepEqual(Buffer.from(result[1]), Buffer.from(`msg_${i}`));
          c.ack(result[0]);
        }

        assert.equal(c.pollSync(50), null);
        q.close();
        ds.close();
      });
    });

    it("empty queue poll timeout returns null", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker-1");

        assert.equal(c.pollSync(50), null);
        q.close();
        ds.close();
      });
    });

    it("push various data types", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker-1");

        const payloads = [Buffer.alloc(0), Buffer.from([0x00, 0x01, 0x02, 0xff]), Buffer.alloc(1000, 0x61)];
        for (const data of payloads) {
          const ts = q.push(data);
          const result = c.pollSync(50);
          assert(result !== null);
          assert.deepEqual(Buffer.from(result[1]), data);
          c.ack(ts);
        }

        q.close();
        ds.close();
      });
    });

    it("push with Uint8Array", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker-1");

        const payload = new Uint8Array([1, 2, 3, 4, 5]);
        const ts = q.push(payload);
        const result = c.pollSync(50);
        assert(result !== null);
        assert.deepEqual(new Uint8Array(result[1]), payload);
        c.ack(ts);

        q.close();
        ds.close();
      });
    });
  });

  describe("multiple consumers", () => {
    it("two consumers round-robin", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c1 = q.openConsumer("group", { runningExpiredSeconds: 60, maxRetryCount: 3 });
        const c2 = q.openConsumer("group", { runningExpiredSeconds: 60, maxRetryCount: 3 });

        const ts1 = q.push(Buffer.from("first"));
        const ts2 = q.push(Buffer.from("second"));

        const r1 = c1.pollSync(100);
        assert(r1 !== null);
        assert.equal(r1[0], ts1);

        const r2 = c2.pollSync(100);
        assert(r2 !== null);
        assert.equal(r2[0], ts2);

        c1.ack(ts1);
        c2.ack(ts2);

        q.close();
        ds.close();
      });
    });

    it("lists groups, inspects state, flushes, and close releases pending", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c1 = q.openConsumer("shared");
        const c1Alias = q.openConsumer("shared");
        q.openConsumer("other");

        assert.deepEqual(q.getConsumerGroupNames(), ["other", "shared"]);

        q.push(Buffer.from("first"));
        const first = c1.pollSync(100);
        assert(first !== null);
        assert.equal(first[0], 1n);
        c1.flush();

        const inspected = c1.inspect();
        assert.equal(inspected.info.groupName, "shared");
        assert.equal(inspected.info.runningExpiredSeconds, 900);
        assert.equal(inspected.info.maxRetryCount, 3);
        assert.equal(inspected.state.processedTs, -(2n ** 63n));
        assert.equal(inspected.state.pendingEntries[0].timestamp, 1n);

        c1.close();
        assert.throws(() => c1Alias.pollSync(0));
        assert.throws(() => c1.ack(1n));

        const reopened = q.openConsumer("shared");
        const redelivered = reopened.pollSync(100);
        assert(redelivered !== null);
        assert.equal(redelivered[0], 1n);
        reopened.ack(1n);

        q.close();
        ds.close();
      });
    });

    it("unacked messages redelivered after expire", async () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableBackgroundThread: false });
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c1 = q.openConsumer("worker", { runningExpiredSeconds: 1, maxRetryCount: 2 });

        q.push(Buffer.from("msg"));
        const r1 = c1.pollSync(100);
        assert(r1 !== null);

        await new Promise((resolve) => setTimeout(resolve, 1100));

        const c2 = q.openConsumer("worker", { runningExpiredSeconds: 1, maxRetryCount: 2 });
        const r2 = c2.pollSync(200);
        assert(r2 !== null);
        assert.equal(r2[0], r1[0]);
        c2.ack(r2[0]);

        q.close();
        ds.close();
        store.close();
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });
  });

  describe("async poll", () => {
    it("async poll returns record", async () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableBackgroundThread: false });
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker");

        const ts = q.push(Buffer.from("async-msg"));
        const result = await c.poll(200);
        assert(result !== null);
        assert.equal(result[0], ts);
        assert.deepEqual(Buffer.from(result[1]), Buffer.from("async-msg"));
        c.ack(ts);

        q.close();
        ds.close();
        store.close();
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });

    it("async poll timeout returns null", async () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableBackgroundThread: false });
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker");

        const result = await c.poll(50);
        assert.equal(result, null);

        q.close();
        ds.close();
        store.close();
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });
  });

  describe("pollCallback", () => {
    it("sets callback and receives notifications", async () => {
      const dir = makeTmpDir();
      try {
        const store = Store.open(dir, { enableBackgroundThread: false });
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker");

        try {
          let called = false;
          c.pollCallback(() => {
            called = true;
          });

          q.push(Buffer.from("data"));
          await new Promise((resolve) => setTimeout(resolve, 50));
          c.pollSync(100);

          assert.equal(called, true);
        } finally {
          c.pollCallback(null);
          q.close();
          ds.close();
          store.close();
        }
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });

    it("clear callback with null and re-set", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        const c = q.openConsumer("worker");

        c.pollCallback(() => {});
        c.pollCallback(null);
        c.pollCallback(() => {});
        c.pollCallback(null);

        q.close();
        ds.close();
      });
    });
  });

  describe("closed queue operations", () => {
    it("operations on closed queue throw", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);
        q.close();
        assert.throws(() => q.push(Buffer.from("data")));
        assert.throws(() => q.openConsumer("worker"));
        ds.close();
      });
    });
  });

  describe("dropConsumer", () => {
    it("dropConsumer removes consumer group", () => {
      withStore((store) => {
        store.createDataset("events", "events");
        const ds = store.openDataset("events", "events");
        const q = store.openQueue(ds);

        q.openConsumer("group-a");
        q.dropConsumer("group-a");
        q.openConsumer("group-a");

        q.close();
        ds.close();
      });
    });
  });
});
