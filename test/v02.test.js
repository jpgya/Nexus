import test from "node:test";
import assert from "node:assert/strict";
import {
  ApplicationPeerDiscovery,
  DuplicateTracker,
  HybridRouteManager,
  LanPeerDiscovery,
  ManualPeerDiscovery,
  TransferController,
  WebRtcTransport
} from "../src/index.js";
import { NexusNode } from "../src/node.js";

test("HybridRouteManager creates multiple edge-disjoint cycles and removes failed links", () => {
  const router = new HybridRouteManager({ maxRoutes: 2 });
  router.setTopology(new Map([
    ["a", ["b", "c", "d", "e"]],
    ["b", ["a", "c"]],
    ["c", ["a", "b"]],
    ["d", ["a", "e"]],
    ["e", ["a", "d"]]
  ]));
  const routes = router.getRoutes("a");
  assert.equal(routes.length, 2);
  assert.deepEqual(new Set(routes.flatMap((route) => route.edges)).size, 6);
  const oldGeneration = router.generation;
  assert.equal(router.removeLink("a", "b"), true);
  assert.ok(router.generation > oldGeneration);
  assert.equal(router.getRoutes("a").length, 1);
});

test("DuplicateTracker bounds entries and expires them", () => {
  let now = 100;
  const tracker = new DuplicateTracker({ maxEntries: 2, now: () => now });
  assert.equal(tracker.mark("first", 200), true);
  assert.equal(tracker.mark("first", 200), false);
  assert.equal(tracker.duplicates, 1);
  tracker.mark("second", 300);
  tracker.mark("third", 300);
  assert.equal(tracker.size, 2);
  now = 250;
  assert.equal(tracker.has("first"), false);
});

test("TransferController retries temporary errors and accounts wire bytes", async () => {
  let attempts = 0;
  const transport = {
    async send() {
      attempts += 1;
      if (attempts === 1) throw new Error("temporary disconnect");
    }
  };
  const transfer = new TransferController(transport, {
    maxMessageBytes: 1024,
    maxQueuedBytesPerPeer: 2048,
    maxRetries: 1,
    retryDelayMs: 0
  });
  const bytes = await transfer.send("peer", { type: "test", payload: new Uint8Array(24) });
  assert.ok(bytes > 24);
  assert.equal(transfer.getStatus().metrics.retries, 1);
  assert.equal(transfer.getStatus().metrics.sentMessages, 1);
});

test("Manual, application, and LAN discovery remain swappable adapters", async () => {
  const manual = new ManualPeerDiscovery();
  const found = [];
  await manual.start((peer) => found.push(peer));
  manual.announce({ peerId: "lan-peer" });
  assert.deepEqual(found, [{ peerId: "lan-peer" }]);
  await manual.stop();

  const calls = [];
  const adapter = {
    start(callback) { calls.push("start"); callback("peer"); },
    stop() { calls.push("stop"); }
  };
  const appDiscovery = new ApplicationPeerDiscovery(adapter);
  const lanDiscovery = new LanPeerDiscovery(adapter);
  await appDiscovery.start(() => {});
  await lanDiscovery.start(() => {});
  await appDiscovery.stop();
  await lanDiscovery.stop();
  assert.deepEqual(calls, ["start", "start", "stop", "stop"]);
});

test("NexusNode memory limits evict old packets and report payload-only usage", async () => {
  const handlers = new Set();
  const transport = {
    send: async () => {},
    request: async () => undefined,
    subscribe(handler) { handlers.add(handler); return () => handlers.delete(handler); }
  };
  const node = new NexusNode(transport, {
    nodeId: "memory-limited",
    maxMemoryBytes: 5,
    autoStartCirculation: false
  });
  await node.publish(new TextEncoder().encode("123456"), {
    dataShards: 2,
    parityShards: 0
  });
  const status = node.getStatus();
  assert.equal(status.packetCount, 1);
  assert.equal(status.packetBytes, 3);
  assert.equal(status.payloadMemory.totalHeapBytes, null);
  assert.equal(status.measurementSource, "runtime");
  node.close();
});

test("WebRtcTransport waits for buffered amount to drain before sending", async () => {
  class BufferedPeerConnection {
    constructor() {
      this.iceGatheringState = "complete";
      this.connectionState = "new";
      this.iceConnectionState = "new";
    }
    addEventListener() {}
    removeEventListener() {}
    async createOffer() { return { type: "offer", sdp: "offer" }; }
    async setLocalDescription(description) { this.localDescription = description; }
    createDataChannel() {
      const listeners = new Map();
      this.channel = {
        readyState: "open",
        bufferedAmount: 20_000,
        addEventListener(type, handler) {
          const current = listeners.get(type) ?? new Set();
          current.add(handler);
          listeners.set(type, current);
        },
        removeEventListener(type, handler) {
          listeners.get(type)?.delete(handler);
        },
        send(frame) { this.sent = frame; },
        close() {}
      };
      this.dispatch = (type) => {
        for (const handler of listeners.get(type) ?? []) handler();
      };
      return this.channel;
    }
    close() {}
  }

  const transport = new WebRtcTransport({
    RTCPeerConnection: BufferedPeerConnection,
    maxMessageBytes: 4096,
    requestTimeoutMs: 1000
  });
  await transport.createOffer("peer-b");
  const connection = [...transport.peers.values()][0].connection;
  const sending = transport.send("peer-b", { type: "test" });
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(connection.channel.sent, undefined);
  connection.channel.bufferedAmount = 0;
  connection.dispatch("bufferedamountlow");
  await sending;
  assert.ok(connection.channel.sent.includes('"type":"test"'));
  transport.close();
});
