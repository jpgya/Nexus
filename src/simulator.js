import { createPackets } from "./packet.js";
import { retrieveObject } from "./retrieval.js";

export class NetworkSimulator {
  constructor({ maxPacketsPerNode = 10_000, random = Math.random } = {}) {
    if (!Number.isInteger(maxPacketsPerNode) || maxPacketsPerNode < 1) {
      throw new RangeError("maxPacketsPerNode must be a positive integer");
    }
    this.maxPacketsPerNode = maxPacketsPerNode;
    this.random = random;
    this.nodes = new Map();
    this.partitions = null;
    this.routes = [];
    this.routeEpoch = 0;
    this.metrics = { forwarded: 0, dropped: 0, duplicates: 0, retrievals: 0 };
  }

  addNode(id) {
    if (typeof id !== "string" || id.length === 0) throw new TypeError("Node id is required");
    if (this.nodes.has(id)) throw new Error(`Node already exists: ${id}`);
    this.nodes.set(id, { id, online: true, packets: new Map() });
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
    const groups = new Map();
    for (const node of this.nodes.values()) {
      if (!node.online) continue;
      const group = this.partitions?.get(node.id) ?? (this.partitions ? -1 : 0);
      if (group === -1) continue;
      const members = groups.get(group) ?? [];
      members.push(node.id);
      groups.set(group, members);
    }
    this.routes = [...groups.values()]
      .filter((members) => members.length >= 2)
      .map((nodes, index) => ({ id: `route-${this.routeEpoch}-${index}`, nodes }));
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
            continue;
          }
          if (this.random() < lossRate) {
            dropped += 1;
            continue;
          }
          const wasPresent = destination.packets.has(packet.packetId);
          this.#store(destination, packet);
          if (wasPresent) duplicates += 1;
          else forwarded += 1;
          if (this.random() < duplicateRate) {
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
    return retrieveObject(objectId, packets, {
      reachable: visibleNodes.length > 0,
      authenticator,
      now
    });
  }

  getStatus() {
    return {
      nodes: [...this.nodes.values()].map(({ id, online, packets }) => ({
        id, online, packetCount: packets.size
      })),
      routes: this.routes.map(({ id, nodes }) => ({ id, nodes: [...nodes] })),
      routeEpoch: this.routeEpoch,
      metrics: { ...this.metrics }
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
    if (node.packets.size >= this.maxPacketsPerNode) return false;
    node.packets.set(packet.packetId, packet);
    return true;
  }

  #validateRate(rate, name) {
    if (!Number.isFinite(rate) || rate < 0 || rate > 1) {
      throw new RangeError(`${name} must be between 0 and 1`);
    }
  }
}
