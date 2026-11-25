import test from "node:test";
import assert from "node:assert/strict";
import { NexusNode } from "../src/node.js";
import { WebRtcTransport } from "../src/network/webrtc-transport.js";

class MemoryNetwork {
  constructor() {
    this.handlers = new Map();
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
  assert.equal(requester.getStatus().packetCount, 7);
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

    async createOffer() { return { type: "offer", sdp: "test-offer" }; }
    async createAnswer() { return { type: "answer", sdp: "test-answer" }; }
    async setLocalDescription(description) { this.localDescription = description; }
    async setRemoteDescription(description) { this.remoteDescription = description; }
    close() { this.connectionState = "closed"; }
  }

  const offerSide = new WebRtcTransport({ RTCPeerConnection: MockPeerConnection });
  const answerSide = new WebRtcTransport({ RTCPeerConnection: MockPeerConnection });
  const offer = await offerSide.createOffer("answer");
  assert.equal(offer, "test-offer");
  const answer = await answerSide.acceptOffer("offer", offer);
  assert.equal(answer, "test-answer");
  await offerSide.acceptAnswer("answer", answer);
  assert.equal(offerSide.getPeers()[0].connectionState, "new");
  offerSide.close();
  answerSide.close();
});
