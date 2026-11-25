import { createPackets } from "./packet.js";
import { retrieveObject } from "./retrieval.js";
import { HybridRouteManager } from "./routing/route-manager.js";

export class NetworkSimulator {
  constructor({
    maxPacketsPerNode = 10_000,
    maxMemoryBytesPerNode = 128 * 1024 * 1024,
    random = Math.random
  } = {}) {
    if (!Number.isInteger(maxPacketsPerNode) || maxPacketsPerNode < 1
      || !Number.isInteger(maxMemoryBytesPerNode) || maxMemoryBytesPerNode < 1) {
      throw new RangeError("Node packet and memory limits must be positive integers");
    }
    this.maxPacketsPerNode = maxPacketsPerNode;
    this.maxMemoryBytesPerNode = maxMemoryBytesPerNode;
    this.random = random;
    this.nodes = new Map();
    this.routeManager = new HybridRouteManager({ maxRoutes: 2, minRouteLength: 2 });
    this.partitions = null;
    this.routes = [];
    this.routeEpoch = 0;
    this.metrics = {
      forwarded: 0,
      dropped: 0,
      duplicates: 0,
      retrievals: 0,
      retrievalSuccesses: 0,
      retrievalDurationMs: 0,
      routeReconfigurations: 0,
      communicationBytes: 0
    };
  }

  addNode(id) {
    if (typeof id !== "string" || id.length === 0) throw new TypeError("Node id is required");
    if (this.nodes.has(id)) throw new Error(`Node already exists: ${id}`);
    this.nodes.set(id, { id, online: true, packets: new Map(), packetBytes: 0 });
    this.reconfigureRoutes();
  }

  setNodeOnline(id, online) {
    const node = this.#node(id);
    node.online = Boolean(online);
    this.reconfigureRoutes();
  }

  partition(groups) {
    const assignments = new Map();
    groups.forEach((group, groupIndex) => {
      for (const id of group) {
        this.#node(id);
        if (assignments.has(id)) throw new Error(`Node appears in multiple partitions: ${id}`);
        assignments.set(id, groupIndex);
      }
    });
    this.partitions = assignments;
    this.reconfigureRoutes();
  }

  healPartitions() {
    this.partitions = null;
    this.reconfigureRoutes();
  }

  reconfigureRoutes() {
    this.routeEpoch += 1;
    this.metrics.routeReconfigurations += 1;
    const groups = new Map();
    for (const node of this.nodes.values()) {
      if (!node.online) continue;
      const group = this.partitions?.get(node.id) ?? (this.partitions ? -1 : 0);
      if (group === -1) continue;
      const members = groups.get(group) ?? [];
      members.push(node.id);
      groups.set(group, members);
    }
    const topology = new Map();
    for (const members of groups.values()) {
      for (const nodeId of members) {
        topology.set(nodeId, new Set(members.filter((peerId) => peerId !== nodeId)));
      }
    }
    this.routeManager.setTopology(topology);
    const routes = [];
    for (const members of groups.values()) {
      if (members.length < 2) continue;
      const route = this.routeManager.getRoutes(members[0], { maxRoutes: 1 })[0];
      if (route) routes.push({ id: `route-${this.routeEpoch}-${routes.length}`, nodes: route.nodes });
    }
    this.routes = routes;
    return this.routes.map((route) => ({ id: route.id, nodes: [...route.nodes] }));
  }

  async addObject(data, options) {
    if (this.nodes.size === 0) throw new Error("Add at least one node before adding an object");
    const packets = await createPackets(data, { ...options, routeEpoch: this.routeEpoch });
    const recipients = [...this.nodes.values()].filter((node) => node.online);
    for (const packet of packets) {
      for (const route of this.routes) {
        const node = this.nodes.get(route.nodes[packet.sequenceNumber % route.nodes.length]);
        this.#store(node, packet);
      }
      if (this.routes.length === 0 && recipients.length > 0) {
        this.#store(recipients[packet.sequenceNumber % recipients.length], packet);
      }
    }
    return { objectId: packets[0].objectId, packetsCreated: packets.length };
  }

  async step({ lossRate = 0, duplicateRate = 0 } = {}) {
    this.#validateRate(lossRate, "lossRate");
    this.#validateRate(duplicateRate, "duplicateRate");
    let forwarded = 0;
    let dropped = 0;
    let duplicates = 0;

    for (const route of this.routes) {
      for (let index = 0; index < route.nodes.length; index += 1) {
        const source = this.nodes.get(route.nodes[index]);
        const destination = this.nodes.get(route.nodes[(index + 1) % route.nodes.length]);
        if (!source?.online || !destination?.online) continue;
        for (const packet of source.packets.values()) {
          if (packet.expiresAt <= Date.now()) {
            source.packets.delete(packet.packetId);
            source.packetBytes -= packet.payload.byteLength;
            continue;
          }
          this.metrics.communicationBytes += packet.payload.byteLength;
          if (this.random() < lossRate) {
            dropped += 1;
            continue;
          }
          const wasPresent = destination.packets.has(packet.packetId);
          this.#store(destination, packet);
          if (wasPresent) duplicates += 1;
          else {
            forwarded += 1;
          }
          if (this.random() < duplicateRate) {
            this.metrics.communicationBytes += packet.payload.byteLength;
            if (destination.packets.has(packet.packetId)) duplicates += 1;
            else this.#store(destination, packet);
          }
        }
      }
    }
    this.metrics.forwarded += forwarded;
    this.metrics.dropped += dropped;
    this.metrics.duplicates += duplicates;
    return { forwarded, dropped, duplicates, routes: this.routes.length };
  }

  async retrieve(objectId, { fromNodeId, authenticator, now } = {}) {
    const startedAt = Date.now();
    this.metrics.retrievals += 1;
    let visibleNodes = [...this.nodes.values()].filter((node) => node.online);
    if (fromNodeId !== undefined) {
      const requester = this.#node(fromNodeId);
      if (!requester.online) visibleNodes = [];
      else if (this.partitions) {
        const group = this.partitions.get(fromNodeId);
        visibleNodes = visibleNodes.filter((node) => this.partitions.get(node.id) === group);
      }
    }
    const packets = visibleNodes.flatMap((node) => [...node.packets.values()]);
    const result = await retrieveObject(objectId, packets, {
      reachable: visibleNodes.length > 0,
      authenticator,
      now
    });
    this.metrics.retrievalDurationMs += Date.now() - startedAt;
    if (result.status === "recovered") this.metrics.retrievalSuccesses += 1;
    return {
      ...result,
      measured: {
        durationMs: Date.now() - startedAt,
        retrievalSuccessRate: this.metrics.retrievalSuccesses / this.metrics.retrievals,
        averageRetrievalMs: this.metrics.retrievalDurationMs / this.metrics.retrievals
      }
    };
  }

  getStatus() {
    return {
      nodes: [...this.nodes.values()].map(({ id, online, packets, packetBytes }) => ({
        id, online, packetCount: packets.size, packetBytes
      })),
      routes: this.routes.map(({ id, nodes }) => ({ id, nodes: [...nodes] })),
      routeEpoch: this.routeEpoch,
      metrics: {
        ...this.metrics,
        peerCount: [...this.nodes.values()].filter((node) => node.online).length,
        routeCount: this.routes.length,
        retrievalSuccessRate: this.metrics.retrievals === 0 ? 0
          : this.metrics.retrievalSuccesses / this.metrics.retrievals,
        averageRetrievalMs: this.metrics.retrievals === 0 ? 0
          : this.metrics.retrievalDurationMs / this.metrics.retrievals,
        circulatingPackets: [...this.nodes.values()].reduce(
          (total, node) => total + node.packets.size, 0)
      },
      measurementSource: "simulation",
      memoryBytes: [...this.nodes.values()].reduce((total, node) => total + node.packetBytes, 0)
    };
  }

  #node(id) {
    const node = this.nodes.get(id);
    if (!node) throw new Error(`Unknown node: ${id}`);
    return node;
  }

  #store(node, packet) {
    if (!node || !node.online) return false;
    if (node.packets.has(packet.packetId)) return false;
    if (packet.payload.byteLength > this.maxMemoryBytesPerNode) return false;
    while (node.packets.size >= this.maxPacketsPerNode
      || node.packetBytes + packet.payload.byteLength > this.maxMemoryBytesPerNode) {
      const oldest = node.packets.keys().next().value;
      if (oldest === undefined) return false;
      node.packetBytes -= node.packets.get(oldest).payload.byteLength;
      node.packets.delete(oldest);
    }
    node.packets.set(packet.packetId, packet);
    node.packetBytes += packet.payload.byteLength;
    return true;
  }

  #validateRate(rate, name) {
    if (!Number.isFinite(rate) || rate < 0 || rate > 1) {
      throw new RangeError(`${name} must be between 0 and 1`);
    }
  }
}
