import test from "node:test";
import assert from "node:assert/strict";
import { NexusNode } from "../src/node.js";
import { WebRtcTransport } from "../src/network/webrtc-transport.js";

class MemoryNetwork {
  constructor({ asyncDelivery = false } = {}) {
    this.handlers = new Map();
    this.asyncDelivery = asyncDelivery;
    this.errors = [];
  }

  transport(id) {
    const network = this;
    const listeners = new Set();
    const transport = {
      subscribe(handler) {
        listeners.add(handler);
        return () => listeners.delete(handler);
      },
      async send(peerId, message) {
        const peerListeners = network.handlers.get(peerId);
        if (!peerListeners) throw new Error(`Unknown peer: ${peerId}`);
        if (network.asyncDelivery) {
          queueMicrotask(() => {
            Promise.all([...peerListeners].map((handler) => handler(id, message)))
              .catch((error) => network.errors.push(error));
          });
          return;
        }
        await Promise.all([...peerListeners].map((handler) => handler(id, message)));
      },
      async request(peerId, message) {
        const peerListeners = network.handlers.get(peerId);
        if (!peerListeners) throw new Error(`Unknown peer: ${peerId}`);
        let response;
        for (const handler of peerListeners) {
          const result = await handler(id, message);
          if (result !== undefined) response = result;
        }
        return response;
      }
    };
    network.handlers.set(id, listeners);
    return transport;
  }
}

test("NexusNode forwards verified packets through peers and retrieves remotely", async () => {
  const network = new MemoryNetwork();
  const transportA = network.transport("a");
  const transportB = network.transport("b");
  const transportC = network.transport("c");
  const a = new NexusNode(transportA, { nodeId: "a" });
  const b = new NexusNode(transportB, { nodeId: "b" });
  const c = new NexusNode(transportC, { nodeId: "c" });
  a.addPeer("b");
  b.addPeer("a");
  b.addPeer("c");
  c.addPeer("b");

  const input = new TextEncoder().encode("real peer protocol over a transport");
  const published = await a.publish(input, { dataShards: 3, parityShards: 2 });
  assert.equal(c.getStatus().packetCount, 5);
  assert.ok(b.getStatus().metrics.relayed > 0);

  a.close();
  const recovered = await c.retrieve(published.objectId);
  assert.equal(recovered.status, "recovered");
  assert.deepEqual(recovered.data, input);

  b.close();
  c.close();
});

test("NexusNode retrieves shards through multiple peers without query loops", async () => {
  const network = new MemoryNetwork();
  const source = new NexusNode(network.transport("source"), { nodeId: "source" });
  const relay = new NexusNode(network.transport("relay"), { nodeId: "relay" });
  const requester = new NexusNode(network.transport("requester"), { nodeId: "requester" });
  const input = new TextEncoder().encode("distributed query across an intermediate peer");
  const published = await source.publish(input, { dataShards: 3, parityShards: 2 });

  source.addPeer("relay");
  relay.addPeer("source");
  relay.addPeer("requester");
  requester.addPeer("relay");
  const result = await requester.retrieve(published.objectId, { maxHops: 3, maxPeers: 2 });
  assert.equal(result.status, "recovered");
  assert.deepEqual(result.data, input);
  assert.ok(result.measured.durationMs >= 0);

  source.close();
  relay.close();
  requester.close();
});

test("independent peer ring continuously circulates packets until configured expiry", async () => {
  const network = new MemoryNetwork({ asyncDelivery: true });
  const ids = ["a", "b", "c", "d"];
  const nodes = ids.map((id) => new NexusNode(network.transport(id), {
    nodeId: id,
    autoStartCirculation: false,
    circulationIntervalMs: 100,
    maxCircuits: 20
  }));
  for (let index = 0; index < ids.length; index += 1) {
    const node = nodes[index];
    const next = ids[(index + 1) % ids.length];
    const previous = ids[(index + ids.length - 1) % ids.length];
    node.addPeer(next);
    node.addPeer(previous);
  }
  await new Promise((resolve) => setTimeout(resolve, 30));
  const published = await nodes[0].publish(new TextEncoder().encode("circulate around peers"), {
    dataShards: 2,
    parityShards: 1,
    expiresInMs: 10_000
  });
  assert.ok(nodes[0].getStatus().routeCount > 0);
  nodes[0].startCirculation();
  await new Promise((resolve) => setTimeout(resolve, 350));

  const status = nodes[0].getStatus();
  assert.ok(status.circulation.metrics.laps > 0, JSON.stringify(nodes.map((node) => node.getStatus())));
  assert.ok(status.metrics.communicationBytes > 0);
  assert.ok(nodes[2].getStatus().packetCount > 0);
  assert.equal(status.measurementSource, "runtime");
  assert.ok((await nodes[2].retrieve(published.objectId)).status === "recovered");
  nodes[0].removePeer("b");
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(nodes[0].getStatus().routeCount, 0);
  nodes[0].addPeer("b");
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.ok(nodes[0].getStatus().routeCount > 0);
  assert.deepEqual(network.errors, []);
  for (const node of nodes) node.close();
});

test("NexusNode rejects corrupted packets before storing or forwarding", async () => {
  const network = new MemoryNetwork();
  const sourceTransport = network.transport("source");
  const receiverTransport = network.transport("receiver");
  const receiver = new NexusNode(receiverTransport, { nodeId: "receiver" });
  const { createPackets } = await import("../src/packet.js");
  const [packet] = await createPackets(new TextEncoder().encode("valid"), {
    dataShards: 1, parityShards: 0
  });
  await sourceTransport.send("receiver", {
    type: "packet",
    hops: 0,
    packet: { ...packet, payload: new TextEncoder().encode("tampered") }
  });
  assert.equal(receiver.getStatus().packetCount, 0);
  assert.equal(receiver.getStatus().metrics.rejected, 1);
  receiver.close();
});

test("NexusNode paginates large remote retrieval responses", async () => {
  const network = new MemoryNetwork();
  const source = new NexusNode(network.transport("source"), {
    nodeId: "source",
    maxRetrieveBytes: 6_000
  });
  const requester = new NexusNode(network.transport("requester"), { nodeId: "requester" });
  requester.addPeer("source");
  const input = new Uint8Array(6_000).map((_, index) => index % 251);
  const published = await source.publish(input, { dataShards: 5, parityShards: 2 });
  const recovered = await requester.retrieve(published.objectId);
  assert.equal(recovered.status, "recovered");
  assert.deepEqual(recovered.data, input);
  assert.ok(requester.getStatus().packetCount >= 5);
  source.close();
  requester.close();
});

test("WebRtcTransport validates browser capability and message limits", () => {
  assert.throws(() => new WebRtcTransport({ RTCPeerConnection: undefined }), /not available/);
  assert.throws(() => new WebRtcTransport({
    RTCPeerConnection: class {},
    maxMessageBytes: 0
  }), /maxMessageBytes/);
});

test("WebRtcTransport supports manual offer-answer signaling through RTC adapter", async () => {
  class MockPeerConnection {
    constructor() {
      MockPeerConnection.instances.push(this);
      this.iceGatheringState = "complete";
      this.connectionState = "new";
      this.iceConnectionState = "new";
      this.listeners = new Map();
    }

    addEventListener(type, handler) {
      const handlers = this.listeners.get(type) ?? new Set();
      handlers.add(handler);
      this.listeners.set(type, handlers);
    }

    removeEventListener(type, handler) {
      this.listeners.get(type)?.delete(handler);
    }

    createDataChannel(label) {
      this.channel = {
        label,
        readyState: "connecting",
        addEventListener() {},
        close() {}
      };
      return this.channel;
    }

    async createOffer() { return { type: "offer", sdp: "test-offer\n" }; }
    async createAnswer() { return { type: "answer", sdp: "test-answer\n" }; }
    async setLocalDescription(description) { this.localDescription = description; }
    async setRemoteDescription(description) { this.remoteDescription = description; }
    close() { this.connectionState = "closed"; }
  }
  MockPeerConnection.instances = [];

  const offerSide = new WebRtcTransport({ RTCPeerConnection: MockPeerConnection });
  const answerSide = new WebRtcTransport({ RTCPeerConnection: MockPeerConnection });
  const offer = await offerSide.createOffer("answer");
  assert.equal(offer, "test-offer\n");
  const answer = await answerSide.acceptOffer("offer", offer);
  assert.equal(answer, "test-answer\n");
  assert.equal(MockPeerConnection.instances[1].remoteDescription.sdp, offer);
  await offerSide.acceptAnswer("answer", answer);
  assert.equal(MockPeerConnection.instances[0].remoteDescription.sdp, answer);
  assert.equal(offerSide.getPeers()[0].connectionState, "new");
  offerSide.close();
  answerSide.close();
});

test("WebRtcTransport closes incomplete peer connections after signaling errors", async () => {
  class FailingPeerConnection {
    constructor() {
      this.iceGatheringState = "complete";
      this.connectionState = "new";
      this.iceConnectionState = "new";
      this.listeners = new Map();
    }

    addEventListener(type, handler) {
      const handlers = this.listeners.get(type) ?? new Set();
      handlers.add(handler);
      this.listeners.set(type, handlers);
    }

    removeEventListener(type, handler) {
      this.listeners.get(type)?.delete(handler);
    }

    createDataChannel() {
      return { addEventListener() {}, close() {} };
    }

    async createOffer() { throw new Error("signaling failed"); }
    close() { this.connectionState = "closed"; }
  }

  const transport = new WebRtcTransport({ RTCPeerConnection: FailingPeerConnection });
  await assert.rejects(transport.createOffer("peer-b"), /signaling failed/);
  assert.deepEqual(transport.getPeers(), []);
  transport.close();
});
