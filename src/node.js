import { createPackets } from "./packet.js";
import { retrieveObject } from "./retrieval.js";
import { validatePacketIntegrity } from "./integrity.js";

export class NexusNode {
  constructor(transport, {
    nodeId = globalThis.crypto?.randomUUID?.() ?? `nexus-${Date.now()}`,
    maxPackets = 10_000,
    maxPacketBytes = 512_000,
    maxRetrievePackets = 255,
    maxRetrieveBytes = 700_000,
    maxRelayHops = 32,
    authenticator
  } = {}) {
    if (!transport || typeof transport.subscribe !== "function"
      || typeof transport.send !== "function" || typeof transport.request !== "function") {
      throw new TypeError("transport must implement send, request, and subscribe");
    }
    if (!Number.isInteger(maxPackets) || maxPackets < 1
      || !Number.isInteger(maxPacketBytes) || maxPacketBytes < 1
      || !Number.isInteger(maxRetrievePackets) || maxRetrievePackets < 1
      || !Number.isInteger(maxRetrieveBytes) || maxRetrieveBytes < 1
      || !Number.isInteger(maxRelayHops) || maxRelayHops < 0) {
      throw new RangeError("Node resource limits must be positive integers");
    }
    this.nodeId = nodeId;
    this.transport = transport;
    this.maxPackets = maxPackets;
    this.maxPacketBytes = maxPacketBytes;
    this.maxRetrievePackets = maxRetrievePackets;
    this.maxRetrieveBytes = maxRetrieveBytes;
    this.maxRelayHops = maxRelayHops;
    this.authenticator = authenticator;
    this.packets = new Map();
    this.peers = new Set();
    this.listeners = new Set();
    this.metrics = { accepted: 0, rejected: 0, relayed: 0, retrievals: 0 };
    this.unsubscribe = transport.subscribe((peerId, message) => this.#handle(peerId, message));
  }

  async publish(data, options) {
    const packets = await createPackets(data, options);
    const accepted = [];
    for (const packet of packets) {
      if (this.#store(packet)) {
        accepted.push(packet);
        this.metrics.accepted += 1;
      }
    }
    await Promise.all([...this.peers].map(async (peerId) => {
      await Promise.all(accepted.map((packet) =>
        this.#sendPacket(peerId, packet, 0)));
    }));
    return { objectId: packets[0].objectId, packetsCreated: packets.length };
  }

  async retrieve(objectId, { timeoutMs, authenticator, now } = {}) {
    this.metrics.retrievals += 1;
    const peerErrors = [];
    const collected = [...this.packets.values()].filter((packet) => packet.objectId === objectId);
    const peers = [...this.peers];
    let reachable = peers.length === 0;
    await Promise.all(peers.map(async (peerId) => {
      let afterSequenceNumber = -1;
      for (let page = 0; page < this.maxRetrievePackets; page += 1) {
        try {
          const response = await this.transport.request(peerId, {
            type: "retrieve",
            objectId,
            afterSequenceNumber,
            maxBytes: this.maxRetrieveBytes
          }, { timeoutMs });
          reachable = true;
          if (!Array.isArray(response?.packets)) break;
          for (const packet of response.packets) {
            if (await this.#acceptPacket(packet, null, 0, false)) collected.push(packet);
          }
          if (response.done || !Number.isInteger(response.nextSequenceNumber)
            || response.nextSequenceNumber <= afterSequenceNumber) break;
          afterSequenceNumber = response.nextSequenceNumber;
        } catch (error) {
          peerErrors.push(`${peerId}: ${error.message}`);
          break;
        }
      }
    }));
    const result = await retrieveObject(objectId, collected, { reachable, authenticator, now });
    return peerErrors.length > 0 ? { ...result, peerErrors } : result;
  }

  addPeer(peerId) {
    if (typeof peerId !== "string" || peerId.length === 0 || peerId === this.nodeId) {
      throw new TypeError("A distinct, non-empty peer identifier is required");
    }
    this.peers.add(peerId);
  }

  removePeer(peerId) {
    this.peers.delete(peerId);
  }

  async syncPeer(peerId) {
    if (!this.peers.has(peerId)) throw new Error(`Unknown peer: ${peerId}`);
    this.#expirePackets();
    const packets = [...this.packets.values()];
    const results = await Promise.allSettled(packets.map((packet) =>
      this.#sendPacket(peerId, packet, 0)));
    const failed = results.filter((result) => result.status === "rejected");
    if (failed.length > 0) {
      throw new AggregateError(
        failed.map((result) => result.reason),
        `Failed to synchronize ${failed.length} packet(s) with peer "${peerId}"`
      );
    }
    return { packetsSent: packets.length };
  }

  subscribe(listener) {
    if (typeof listener !== "function") throw new TypeError("listener must be a function");
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  getStatus() {
    this.#expirePackets();
    return {
      nodeId: this.nodeId,
      peers: [...this.peers],
      packetCount: this.packets.size,
      metrics: { ...this.metrics }
    };
  }

  close() {
    this.unsubscribe?.();
    this.unsubscribe = null;
    this.listeners.clear();
    this.peers.clear();
    this.packets.clear();
  }

  async #handle(peerId, message) {
    if (!message || typeof message !== "object") return undefined;
    if (message.type === "retrieve") {
      if (typeof message.objectId !== "string") throw new TypeError("Invalid retrieval request");
      if (!Number.isInteger(message.afterSequenceNumber) || message.afterSequenceNumber < -1) {
        throw new TypeError("Invalid retrieval cursor");
      }
      this.#expirePackets();
      const candidates = [...this.packets.values()]
        .filter((packet) => packet.objectId === message.objectId
          && packet.sequenceNumber > message.afterSequenceNumber)
        .sort((left, right) => left.sequenceNumber - right.sequenceNumber);
      const maxBytes = Number.isInteger(message.maxBytes)
        ? Math.min(message.maxBytes, this.maxRetrieveBytes)
        : this.maxRetrieveBytes;
      if (maxBytes < 1) throw new RangeError("Invalid retrieval page byte limit");
      const packets = [];
      let estimatedBytes = 0;
      for (const packet of candidates) {
        const packetBytes = Math.ceil(packet.payload.byteLength * 4 / 3) + 2048;
        if (packets.length >= this.maxRetrievePackets
          || estimatedBytes + packetBytes > maxBytes) break;
        packets.push(packet);
        estimatedBytes += packetBytes;
      }
      const nextSequenceNumber = packets.at(-1)?.sequenceNumber;
      if (packets.length === 0 && candidates.length > 0) {
        throw new RangeError("A shard exceeds the retrieval page byte limit");
      }
      return {
        packets,
        nextSequenceNumber: nextSequenceNumber ?? message.afterSequenceNumber,
        done: packets.length === candidates.length
      };
    }
    if (message.type !== "packet") return undefined;
    await this.#acceptPacket(message.packet, peerId, message.hops, true);
    return { accepted: this.packets.has(message.packet?.packetId) };
  }

  async #acceptPacket(packet, senderId, hops, relay) {
    if (!Number.isInteger(hops) || hops < 0 || hops > this.maxRelayHops
      || !packet || !(packet.payload instanceof Uint8Array)
      || packet.payload.byteLength > this.maxPacketBytes
      || !await validatePacketIntegrity(packet)) {
      this.metrics.rejected += 1;
      return false;
    }
    if (packet.authentication?.scheme === "hmac-sha256") {
      if (!this.authenticator || !await this.authenticator.verify(packet)) {
        this.metrics.rejected += 1;
        return false;
      }
    } else if (packet.authentication?.scheme !== "none") {
      this.metrics.rejected += 1;
      return false;
    }
    if (!this.#store(packet)) return false;
    this.metrics.accepted += 1;
    for (const listener of this.listeners) listener(packet);
    if (relay && hops < this.maxRelayHops) {
      const destinations = [...this.peers].filter((peerId) => peerId !== senderId);
      const results = await Promise.allSettled(destinations.map((peerId) =>
        this.#sendPacket(peerId, packet, hops + 1)));
      this.metrics.relayed += results.filter((result) => result.status === "fulfilled").length;
    }
    return true;
  }

  async #sendPacket(peerId, packet, hops) {
    await this.transport.send(peerId, { type: "packet", packet, hops });
  }

  #store(packet) {
    this.#expirePackets();
    if (this.packets.has(packet.packetId)) return false;
    if (this.packets.size >= this.maxPackets) {
      const oldest = this.packets.keys().next().value;
      if (oldest === undefined) return false;
      this.packets.delete(oldest);
    }
    this.packets.set(packet.packetId, packet);
    return true;
  }

  #expirePackets(now = Date.now()) {
    for (const [packetId, packet] of this.packets) {
      if (packet.expiresAt <= now) this.packets.delete(packetId);
    }
  }
}
