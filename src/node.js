import { createPackets } from "./packet.js";
import { retrieveObject } from "./retrieval.js";
import { validatePacketIntegrity } from "./integrity.js";
import { HybridRouteManager } from "./routing/route-manager.js";
import { DuplicateTracker } from "./circulation/duplicate-tracker.js";
import { CirculationEngine } from "./circulation/circulation-engine.js";
import { TransferController } from "./network/transfer-controller.js";

export class NexusNode {
  constructor(transport, {
    nodeId = globalThis.crypto?.randomUUID?.() ?? `nexus-${Date.now()}`,
    maxPackets = 10_000,
    maxMemoryBytes = 128 * 1024 * 1024,
    maxPacketBytes = 512_000,
    maxPeers = 64,
    maxTopologyNodes = 256,
    maxRetrievePackets = 255,
    maxRetrieveBytes = 700_000,
    maxRelayHops = 32,
    maxRoutes = 2,
    maxSearchHops = 3,
    maxSearchPeers = 2,
    circulationIntervalMs = 5_000,
    maxCircuits = 10_000,
    maxQueuedBytesPerPeer = 4_000_000,
    maxMessageBytes = 1_000_000,
    maxRetries = 1,
    autoStartCirculation = true,
    discovery,
    authenticator
  } = {}) {
    if (!transport || typeof transport.subscribe !== "function"
      || typeof transport.send !== "function" || typeof transport.request !== "function") {
      throw new TypeError("transport must implement send, request, and subscribe");
    }
    if (!Number.isInteger(maxPackets) || maxPackets < 1
      || !Number.isInteger(maxMemoryBytes) || maxMemoryBytes < 1
      || !Number.isInteger(maxPacketBytes) || maxPacketBytes < 1
      || !Number.isInteger(maxPeers) || maxPeers < 1 || maxPeers > 256
      || !Number.isInteger(maxTopologyNodes) || maxTopologyNodes < 1 || maxTopologyNodes > 256
      || !Number.isInteger(maxRetrievePackets) || maxRetrievePackets < 1
      || !Number.isInteger(maxRetrieveBytes) || maxRetrieveBytes < 1
      || !Number.isInteger(maxRelayHops) || maxRelayHops < 0
      || !Number.isInteger(maxRoutes) || maxRoutes < 1
      || !Number.isInteger(maxSearchHops) || maxSearchHops < 0
      || !Number.isInteger(maxSearchPeers) || maxSearchPeers < 1
      || !Number.isInteger(maxMessageBytes) || maxMessageBytes < 4096
      || maxQueuedBytesPerPeer < maxMessageBytes) {
      throw new RangeError("Node resource limits must be positive integers");
    }
    this.nodeId = nodeId;
    this.transport = transport;
    this.maxPackets = maxPackets;
    this.maxMemoryBytes = maxMemoryBytes;
    this.maxPeers = maxPeers;
    this.maxTopologyNodes = maxTopologyNodes;
    this.memoryBytes = 0;
    this.maxRetrievePackets = maxRetrievePackets;
    this.maxRetrieveBytes = maxRetrieveBytes;
    this.maxMessageBytes = maxMessageBytes;
    this.maxPacketBytes = Math.min(maxPacketBytes, Math.floor((maxMessageBytes - 4096) * 0.7));
    this.maxRelayHops = maxRelayHops;
    this.authenticator = authenticator;
    this.maxSearchHops = maxSearchHops;
    this.maxSearchPeers = maxSearchPeers;
    this.discovery = discovery;
    this.packets = new Map();
    this.peers = new Set();
    this.discoveryPeers = new Map();
    this.listeners = new Set();
    this.metrics = {
      accepted: 0,
      rejected: 0,
      expired: 0,
      relayed: 0,
      duplicates: 0,
      dropped: 0,
      routeReconfigurations: 0,
      retrievals: 0,
      retrievalSuccesses: 0,
      retrievalDurationMs: 0
    };
    this.routeManager = new HybridRouteManager({ maxRoutes });
    this.duplicateTracker = new DuplicateTracker({ maxEntries: maxPackets * 2 });
    this.transfer = new TransferController(transport, {
      maxMessageBytes,
      maxQueuedBytesPerPeer,
      maxRetries
    });
    this.topologyRecords = new Map([[nodeId, { version: 0, peers: [] }]]);
    this.topologyVersion = 0;
    this.linkRevision = 0;
    this.linkStates = new Map();
    this.queryCache = new Map();
    this.maxQueryCache = 16;
    this.circulation = new CirculationEngine({
      nodeId,
      routeManager: this.routeManager,
      send: async (peerId, message) => {
        return this.transfer.send(peerId, message);
      },
      intervalMs: circulationIntervalMs,
      maxCircuits,
      maxRelayHops,
      onMetric: (event) => {
        if (event.type === "route-send-failed") this.metrics.dropped += 1;
      }
    });
    this.unsubscribe = transport.subscribe((peerId, message) => this.#handle(peerId, message));
    this.unsubscribeStatus = transport.subscribeStatus?.((status) => {
      const current = new Map((status ?? []).map((peer) => [peer.id, peer]));
      for (const peerId of this.peers) {
        const peer = current.get(peerId);
        if (!peer || ["failed", "closed"].includes(peer.connectionState)
          || ["failed", "closed"].includes(peer.iceConnectionState)) {
          this.removePeer(peerId);
        }
      }
    });
    this.autoStartCirculation = autoStartCirculation;
    this.#refreshTopology();
  }

  async publish(data, options) {
    const packets = await createPackets(data, options);
    if (packets.some((packet) => packet.payload.byteLength > this.maxPacketBytes
      || packet.payload.byteLength > this.maxMemoryBytes)) {
      throw new RangeError(
        `Encoded shard exceeds the node packet (${this.maxPacketBytes}) or memory (${this.maxMemoryBytes}) limit`
      );
    }
    for (const packet of packets) {
      await this.#acceptPacket(packet, null, 0, true);
    }
    this.#ensureCirculation();
    return { objectId: packets[0].objectId, packetsCreated: packets.length };
  }

  async retrieve(objectId, {
    timeoutMs,
    authenticator,
    now,
    maxHops = this.maxSearchHops,
    maxPeers = this.maxSearchPeers
  } = {}) {
    if (typeof objectId !== "string" || objectId.length === 0) {
      throw new TypeError("objectId is required");
    }
    if (!Number.isInteger(maxHops) || maxHops < 0 || maxHops > 8
      || !Number.isInteger(maxPeers) || maxPeers < 1 || maxPeers > 32) {
      throw new RangeError("Search scope must use 0-8 hops and 1-32 peers");
    }
    const startedAt = Date.now();
    this.metrics.retrievals += 1;
    const collected = [...this.packets.values()].filter((packet) => packet.objectId === objectId);
    const peers = [...this.peers].slice(0, maxPeers);
    const requestId = globalThis.crypto?.randomUUID?.()
      ?? `${this.nodeId}:${Date.now()}:${Math.random()}`;
    let reachable = peers.length === 0;
    const peerErrors = [];
    for (const peerId of peers) {
      let afterSequenceNumber = -1;
      for (let page = 0; page < this.maxRetrievePackets; page += 1) {
        try {
          const response = await this.transport.request(peerId, {
            type: "retrieve",
            objectId,
            requestId,
            originId: this.nodeId,
            visited: [this.nodeId],
            hopsRemaining: maxHops,
            maxPeers,
            afterSequenceNumber,
            maxBytes: this.maxRetrieveBytes
          }, { timeoutMs });
          reachable = true;
          if (!Array.isArray(response?.packets)) break;
          for (const packet of response.packets) {
            if (await this.#acceptPacket(packet, null, 0, false)) {
              collected.push(packet);
            } else if (this.packets.has(packet.packetId)) {
              this.metrics.duplicates += 1;
            }
          }
          if (this.#hasEnoughShards(objectId, collected)) break;
          if (response.done || !Number.isInteger(response.nextSequenceNumber)
            || response.nextSequenceNumber <= afterSequenceNumber) break;
          afterSequenceNumber = response.nextSequenceNumber;
        } catch (error) {
          peerErrors.push(`${peerId}: ${error.message}`);
          break;
        }
      }
      if (this.#hasEnoughShards(objectId, collected)) break;
    }
    const result = await retrieveObject(objectId, collected, { reachable, authenticator, now });
    this.metrics.retrievalDurationMs += Date.now() - startedAt;
    if (result.status === "recovered") this.metrics.retrievalSuccesses += 1;
    const withMetrics = {
      ...result,
      measured: {
        durationMs: Date.now() - startedAt,
        retrievalSuccessRate: this.metrics.retrievals === 0 ? 0
          : this.metrics.retrievalSuccesses / this.metrics.retrievals,
        averageRetrievalMs: this.metrics.retrievals === 0 ? 0
          : this.metrics.retrievalDurationMs / this.metrics.retrievals
      }
    };
    return peerErrors.length > 0 ? { ...withMetrics, peerErrors } : withMetrics;
  }

  addPeer(peerId) {
    if (typeof peerId !== "string" || peerId.length === 0 || peerId === this.nodeId) {
      throw new TypeError("A distinct, non-empty peer identifier is required");
    }
    if (this.peers.has(peerId)) return;
    if (this.peers.size >= this.maxPeers) {
      throw new RangeError(`Node cannot configure more than ${this.maxPeers} direct peers`);
    }
    this.peers.add(peerId);
    this.#refreshTopology();
    this.#broadcastTopology();
  }

  removePeer(peerId) {
    if (!this.peers.delete(peerId)) return false;
    this.#refreshTopology();
    this.#broadcastTopology();
    return true;
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

  startCirculation() {
    return this.circulation.start();
  }

  stopCirculation() {
    return this.circulation.stop();
  }

  async startDiscovery(onPeerDiscovered = () => {}) {
    if (!this.discovery) throw new Error("No peer discovery provider was configured");
    if (typeof onPeerDiscovered !== "function") {
      throw new TypeError("onPeerDiscovered must be a function");
    }
    return this.discovery.start((peer) => {
      const peerId = typeof peer === "string" ? peer : peer?.peerId;
      if (typeof peerId !== "string" || !peerId || peerId === this.nodeId) return;
      this.discoveryPeers.set(peerId, peer);
      onPeerDiscovered(peer);
    });
  }

  async stopDiscovery() {
    await this.discovery?.stop();
    this.discoveryPeers.clear();
  }

  getStatus() {
    this.#expirePackets();
    const circulation = this.circulation.getStatus();
    const transfer = this.transfer.getStatus();
    const transportPeers = this.transport.getPeers?.();
    const connectedPeerCount = Array.isArray(transportPeers)
      ? transportPeers.filter((peer) =>
        peer.connectionState === "connected" && peer.dataChannelState === "open").length
      : this.peers.size;
    return {
      nodeId: this.nodeId,
      peers: [...this.peers],
      peerCount: connectedPeerCount,
      configuredPeerCount: this.peers.size,
      discoveredPeerCount: this.discoveryPeers.size,
      packetCount: this.packets.size,
      packetBytes: this.memoryBytes,
      payloadMemory: {
        retainedPayloadBytes: this.memoryBytes,
        totalHeapBytes: null,
        note: "Payload-only lower bound; JavaScript object/map overhead is not measured"
      },
      routes: this.routeManager.getRoutes(this.nodeId),
      routeCount: this.routeManager.getRoutes(this.nodeId).length,
      circulation,
      transfer,
      measurementSource: "runtime",
      metrics: {
        ...this.metrics,
        retrievalSuccessRate: this.metrics.retrievals === 0 ? 0
          : this.metrics.retrievalSuccesses / this.metrics.retrievals,
        averageRetrievalMs: this.metrics.retrievals === 0 ? 0
          : this.metrics.retrievalDurationMs / this.metrics.retrievals,
        circulatingPackets: circulation.circulatingPackets,
        forwarded: circulation.metrics.forwarded + this.metrics.relayed,
        deduplicated: this.duplicateTracker.duplicates,
        communicationBytes: transfer.metrics.sentBytes,
        communicationBytesIsEstimate: true,
        memoryBytes: this.memoryBytes
      }
    };
  }

  close() {
    this.unsubscribe?.();
    this.unsubscribe = null;
    this.unsubscribeStatus?.();
    this.unsubscribeStatus = null;
    this.circulation.close();
    this.listeners.clear();
    this.peers.clear();
    this.packets.clear();
    this.memoryBytes = 0;
    this.duplicateTracker.clear();
    this.queryCache.clear();
    this.stopDiscovery().catch(() => {});
  }

  async #handle(peerId, message) {
    if (!message || typeof message !== "object") return undefined;
    if (message.type === "topology") {
      return this.#handleTopology(peerId, message.records, message.linkStates);
    }
    if (message.type === "circulate") {
      const packet = message.envelope?.packet;
      if (!this.circulation.isValidFrame(peerId, message.envelope)) return { accepted: false };
      const previousPacket = this.packets.get(packet?.packetId);
      const accepted = await this.#acceptPacket(packet, peerId, 0, false);
      if (!accepted && (!previousPacket || previousPacket.integrityHash !== packet?.integrityHash)) {
        return { accepted: false };
      }
      await this.circulation.receive(peerId, message.envelope);
      return { accepted: true };
    }
    if (message.type === "retrieve") {
      return this.#handleRetrievalRequest(peerId, message);
    }
    if (message.type !== "packet") return undefined;
    if (!await this.#acceptPacket(message.packet, peerId, message.hops, true)) {
      return { accepted: false };
    }
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
    if (!this.#store(packet)) {
      if (this.packets.has(packet.packetId)) {
        this.duplicateTracker.mark(packet.packetId, packet.expiresAt);
        this.metrics.duplicates += 1;
      } else this.metrics.rejected += 1;
      return false;
    }
    this.metrics.accepted += 1;
    this.duplicateTracker.mark(packet.packetId, packet.expiresAt);
    if (this.circulation.register(packet)) this.#ensureCirculation();
    for (const listener of this.listeners) listener(packet);
    if (relay && hops < this.maxRelayHops) {
      const destinations = this.#selectRelayPeers(senderId);
      const results = await Promise.allSettled(destinations.map((peerId) =>
        this.#sendPacket(peerId, packet, hops + 1)));
      this.metrics.relayed += results.filter((result) => result.status === "fulfilled").length;
      this.metrics.dropped += results.filter((result) => result.status === "rejected").length;
    }
    return true;
  }

  async #sendPacket(peerId, packet, hops) {
    await this.transfer.send(peerId, { type: "packet", packet, hops });
  }

  #selectRelayPeers(senderId) {
    const routePeers = this.routeManager.getRoutes(this.nodeId)
      .map((route) => route.nodes[1])
      .filter((peerId) => peerId !== senderId);
    const candidates = routePeers.length > 0
      ? routePeers
      : [...this.peers].filter((peerId) => peerId !== senderId).sort();
    return [...new Set(candidates)].slice(0, this.routeManager.maxRoutes);
  }

  #ensureCirculation() {
    if (this.autoStartCirculation && this.packets.size > 0) this.circulation.start();
  }

  #refreshTopology() {
    const peers = [...this.peers].sort();
    const current = this.topologyRecords.get(this.nodeId);
    if (!current || current.peers.join("\0") !== peers.join("\0")) {
      this.topologyVersion += 1;
      const previousPeers = new Set(current?.peers ?? []);
      const nextPeers = new Set(peers);
      for (const peerId of new Set([...previousPeers, ...nextPeers])) {
        if (previousPeers.has(peerId) === nextPeers.has(peerId)) continue;
        const [left, right] = [this.nodeId, peerId].sort();
        this.linkRevision = Math.max(Date.now() * 1000, this.linkRevision + 1);
        this.linkStates.set(`${left}\0${right}`, {
          left,
          right,
          enabled: nextPeers.has(peerId),
          revision: this.linkRevision,
          origin: this.nodeId
        });
      }
      this.topologyRecords.set(this.nodeId, { version: this.topologyVersion, peers });
    }
    this.#rebuildRoutes();
  }

  #rebuildRoutes() {
    const topology = new Map([...this.topologyRecords].map(([nodeId, record]) =>
      [nodeId, new Set(record.peers)]));
    for (const { left, right, enabled } of this.linkStates.values()) {
      if (!topology.has(left)) topology.set(left, new Set());
      if (!topology.has(right)) topology.set(right, new Set());
      if (enabled) {
        topology.get(left).add(right);
        topology.get(right).add(left);
      } else {
        topology.get(left).delete(right);
        topology.get(right).delete(left);
      }
    }
    const previousGeneration = this.routeManager.generation;
    this.routeManager.setTopology(topology);
    if (this.routeManager.generation !== previousGeneration) {
      this.metrics.routeReconfigurations += 1;
      this.#ensureCirculation();
    }
  }

  async #broadcastTopology(exceptPeerId) {
    const records = [...this.topologyRecords].map(([nodeId, record]) => ({
      nodeId,
      version: record.version,
      peers: record.peers
    }));
    const linkStates = [...this.linkStates.values()];
    const targets = [...this.peers].filter((peerId) => peerId !== exceptPeerId);
    await Promise.allSettled(targets.map((peerId) =>
      this.transfer.send(peerId, { type: "topology", records, linkStates })));
  }

  #handleTopology(peerId, records, remoteLinkStates) {
    if (!Array.isArray(records) || records.length > 256) return { accepted: false };
    let changed = false;
    for (const record of records) {
      if (typeof record?.nodeId !== "string" || !record.nodeId
        || !Number.isInteger(record.version) || record.version < 0
        || !Array.isArray(record.peers) || record.peers.length > 64
        || record.peers.some((id) => typeof id !== "string" || !id || id === record.nodeId)) {
        continue;
      }
      const current = this.topologyRecords.get(record.nodeId);
      if (!current || record.version > current.version) {
        if (record.nodeId === this.nodeId) continue;
        if (!current && this.topologyRecords.size >= this.maxTopologyNodes) continue;
        this.topologyRecords.set(record.nodeId, {
          version: record.version,
          peers: [...new Set(record.peers)].sort()
        });
        changed = true;
      }
    }
    if (Array.isArray(remoteLinkStates) && remoteLinkStates.length <= 2048) {
      for (const link of remoteLinkStates) {
        if (typeof link?.left !== "string" || !link.left || typeof link.right !== "string"
          || !link.right || link.left >= link.right || typeof link.enabled !== "boolean"
          || !Number.isFinite(link.revision) || typeof link.origin !== "string") continue;
        const key = `${link.left}\0${link.right}`;
        const current = this.linkStates.get(key);
        if (!current || link.revision > current.revision
          || (link.revision === current.revision && link.origin > current.origin)) {
          this.linkStates.set(key, link);
          changed = true;
        }
      }
    }
    if (changed) {
      this.#rebuildRoutes();
      this.#broadcastTopology(peerId);
    }
    return { accepted: true, changed };
  }

  async #handleRetrievalRequest(peerId, message) {
    if (typeof message.objectId !== "string" || !message.objectId
      || !Number.isInteger(message.afterSequenceNumber) || message.afterSequenceNumber < -1
      || typeof message.requestId !== "string" || !message.requestId || message.requestId.length > 128
      || !Array.isArray(message.visited) || message.visited.length > 32
      || !Number.isInteger(message.hopsRemaining) || message.hopsRemaining < 0
      || message.hopsRemaining > 8
      || !Number.isInteger(message.maxPeers) || message.maxPeers < 1 || message.maxPeers > 32) {
      throw new TypeError("Invalid distributed retrieval request");
    }
    const cacheKey = `${message.requestId}:${message.objectId}:${message.afterSequenceNumber}`;
    const cached = this.queryCache.get(cacheKey);
    if (cached) return cached;
    const operation = this.#collectRetrievalPage(peerId, message);
    this.queryCache.set(cacheKey, operation);
    if (this.queryCache.size > this.maxQueryCache) {
      this.queryCache.delete(this.queryCache.keys().next().value);
    }
    try {
      return await operation;
    } finally {
      setTimeout(() => {
        if (this.queryCache.get(cacheKey) === operation) this.queryCache.delete(cacheKey);
      }, 5_000).unref?.();
    }
  }

  async #collectRetrievalPage(peerId, message) {
    this.#expirePackets();
    const maxBytes = Number.isInteger(message.maxBytes)
      ? Math.min(message.maxBytes, this.maxRetrieveBytes)
      : this.maxRetrieveBytes;
    if (maxBytes < 1) throw new RangeError("Invalid retrieval page byte limit");
    const packets = new Map();
    const add = (packet) => {
      if (packet.sequenceNumber > message.afterSequenceNumber) packets.set(packet.packetId, packet);
    };
    for (const packet of this.packets.values()) {
      if (packet.objectId === message.objectId) add(packet);
    }
    const visited = new Set([...message.visited, this.nodeId]);
    let childDone = true;
    if (message.hopsRemaining > 0 && visited.size <= 32) {
      const nextPeers = [...this.peers]
        .filter((candidate) => candidate !== peerId && !visited.has(candidate))
        .slice(0, Math.min(this.maxSearchPeers, message.maxPeers ?? this.maxSearchPeers));
      const responses = await Promise.allSettled(nextPeers.map((nextPeer) =>
        this.transport.request(nextPeer, {
          ...message,
          visited: [...visited],
          hopsRemaining: message.hopsRemaining - 1
        })));
      for (const response of responses) {
        if (response.status !== "fulfilled" || !Array.isArray(response.value?.packets)) {
          childDone = false;
          continue;
        }
        childDone = childDone && response.value.done === true;
        for (const packet of response.value.packets) {
          if (packet.objectId === message.objectId) add(packet);
        }
      }
    }

    const sorted = [...packets.values()].sort((left, right) =>
      left.sequenceNumber - right.sequenceNumber);
    const selected = [];
    let estimatedBytes = 0;
    for (const packet of sorted) {
      const packetBytes = Math.ceil(packet.payload.byteLength * 4 / 3) + 2048;
      if (selected.length >= this.maxRetrievePackets || estimatedBytes + packetBytes > maxBytes) break;
      selected.push(packet);
      estimatedBytes += packetBytes;
    }
    if (selected.length === 0 && sorted.length > 0) {
      throw new RangeError("A shard exceeds the retrieval page byte limit");
    }
    const nextSequenceNumber = selected.at(-1)?.sequenceNumber;
    const done = selected.length === sorted.length && childDone;
    return {
      packets: selected,
      nextSequenceNumber: nextSequenceNumber ?? message.afterSequenceNumber,
      done
    };
  }

  #hasEnoughShards(objectId, packets) {
    const matching = packets.filter((packet) => packet.objectId === objectId);
    const encoding = /^rs-gf256-v1:(\d+):(\d+):(\d+)$/.exec(matching[0]?.encodingId ?? "");
    if (!encoding) return false;
    const needed = Number(encoding[1]);
    return new Set(matching.map((packet) => packet.sequenceNumber)).size >= needed;
  }

  #store(packet) {
    this.#expirePackets();
    if (this.packets.has(packet.packetId)) return false;
    if (packet.payload.byteLength > this.maxMemoryBytes) return false;
    while (this.packets.size >= this.maxPackets
      || this.memoryBytes + packet.payload.byteLength > this.maxMemoryBytes) {
      const oldest = this.packets.keys().next().value;
      if (oldest === undefined) return false;
      this.memoryBytes -= this.packets.get(oldest).payload.byteLength;
      this.packets.delete(oldest);
      this.circulation.unregister(oldest);
      this.duplicateTracker.entries.delete(oldest);
    }
    this.packets.set(packet.packetId, packet);
    this.memoryBytes += packet.payload.byteLength;
    return true;
  }

  #expirePackets(now = Date.now()) {
    for (const [packetId, packet] of this.packets) {
      if (packet.expiresAt <= now) {
        this.packets.delete(packetId);
        this.memoryBytes -= packet.payload.byteLength;
        this.duplicateTracker.entries.delete(packetId);
        this.circulation.unregister(packetId);
        this.metrics.expired += 1;
      }
    }
  }
}
