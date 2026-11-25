export class HybridRouteManager {
  constructor({ maxRoutes = 2, maxRouteLength = 16, minRouteLength = 3 } = {}) {
    if (!Number.isInteger(maxRoutes) || maxRoutes < 1
      || !Number.isInteger(maxRouteLength) || maxRouteLength < 2
      || !Number.isInteger(minRouteLength) || minRouteLength < 2
      || minRouteLength > maxRouteLength) {
      throw new RangeError("Route limits must be positive integers");
    }
    this.maxRoutes = maxRoutes;
    this.maxRouteLength = maxRouteLength;
    this.minRouteLength = minRouteLength;
    this.graph = new Map();
    this.generation = 0;
    this.topologyId = "empty";
  }

  setTopology(topology) {
    if (!(topology instanceof Map)) throw new TypeError("topology must be a Map");
    const graph = new Map();
    for (const [node, peers] of topology) {
      if (typeof node !== "string" || !node || !(peers instanceof Set || Array.isArray(peers))) {
        throw new TypeError("Invalid topology entry");
      }
      graph.set(node, new Set());
    }
    for (const [node, peers] of topology) {
      for (const peer of peers) {
        if (typeof peer !== "string" || !peer || peer === node) continue;
        if (!graph.has(peer)) graph.set(peer, new Set());
        graph.get(node).add(peer);
        graph.get(peer).add(node);
      }
    }
    if (this.#sameGraph(graph)) return this.generation;
    this.graph = graph;
    this.generation += 1;
    this.topologyId = this.#fingerprint(graph);
    return this.generation;
  }

  addLink(left, right) {
    this.#validateNode(left);
    this.#validateNode(right);
    if (left === right) throw new RangeError("A route cannot contain a self-link");
    const changed = !this.graph.get(left)?.has(right);
    if (!this.graph.has(left)) this.graph.set(left, new Set());
    if (!this.graph.has(right)) this.graph.set(right, new Set());
    this.graph.get(left).add(right);
    this.graph.get(right).add(left);
    if (changed) {
      this.generation += 1;
      this.topologyId = this.#fingerprint(this.graph);
    }
    return changed;
  }

  removeLink(left, right) {
    const changed = Boolean(this.graph.get(left)?.delete(right)
      || this.graph.get(right)?.has(left));
    this.graph.get(right)?.delete(left);
    if (changed) {
      this.generation += 1;
      this.topologyId = this.#fingerprint(this.graph);
    }
    return changed;
  }

  getRoutes(localNodeId, { maxRoutes = this.maxRoutes } = {}) {
    this.#validateNode(localNodeId);
    if (!Number.isInteger(maxRoutes) || maxRoutes < 1) {
      throw new RangeError("maxRoutes must be a positive integer");
    }
    if (!this.graph.has(localNodeId)) return [];

    const cycles = [];
    const visited = new Set([localNodeId]);
    const path = [localNodeId];
    let explored = 0;
    const visit = (node) => {
      if (cycles.length >= 128 || explored >= 10_000) return;
      const peers = [...(this.graph.get(node) ?? [])].sort();
      for (const peer of peers) {
        explored += 1;
        if (explored > 10_000) return;
        if (peer === localNodeId && path.length >= this.minRouteLength) {
          cycles.push([...path]);
        } else if (!visited.has(peer) && path.length < this.maxRouteLength) {
          visited.add(peer);
          path.push(peer);
          visit(peer);
          path.pop();
          visited.delete(peer);
        }
      }
    };
    visit(localNodeId);

    cycles.sort((left, right) => left.length - right.length
      || left.join("\0").localeCompare(right.join("\0")));
    const selected = [];
    const usedEdges = new Set();
    for (const cycle of cycles) {
      const edges = cycle.map((node, index) => {
        const peer = cycle[(index + 1) % cycle.length];
        return node < peer ? `${node}\0${peer}` : `${peer}\0${node}`;
      });
      if (edges.some((edge) => usedEdges.has(edge))) continue;
      selected.push({
        id: this.getRouteId(cycle),
        generation: this.topologyId,
        nodes: cycle,
        edges: [...edges]
      });
      for (const edge of edges) usedEdges.add(edge);
      if (selected.length >= maxRoutes) break;
    }
    return selected;
  }

  getTopology() {
    return new Map([...this.graph].map(([node, peers]) => [node, new Set(peers)]));
  }

  getRouteId(nodes, topologyId = this.topologyId) {
    const forward = this.#canonicalRotation(nodes);
    const reverse = this.#canonicalRotation([...nodes].reverse());
    const canonical = forward < reverse ? forward : reverse;
    return `route-${topologyId}-${canonical}`;
  }

  isValidRoute(nodes, topologyId = this.topologyId, routeId) {
    if (topologyId !== this.topologyId || !Array.isArray(nodes)
      || nodes.length < 3 || nodes.length > this.maxRouteLength
      || new Set(nodes).size !== nodes.length) return false;
    if (routeId !== undefined && routeId !== this.getRouteId(nodes, topologyId)) return false;
    return nodes.every((node, index) =>
      this.graph.get(node)?.has(nodes[(index + 1) % nodes.length]) === true);
  }

  #validateNode(node) {
    if (typeof node !== "string" || node.length === 0) throw new TypeError("Node ID is required");
  }

  #sameGraph(other) {
    if (this.graph.size !== other.size) return false;
    for (const [node, peers] of other) {
      const current = this.graph.get(node);
      if (!current || current.size !== peers.size) return false;
      for (const peer of peers) if (!current.has(peer)) return false;
    }
    return true;
  }

  #fingerprint(graph) {
    return [...graph]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([node, peers]) => `${node}:${[...peers].sort().join(",")}`)
      .join("|");
  }

  #canonicalRotation(nodes) {
    const rotations = nodes.map((_, index) =>
      [...nodes.slice(index), ...nodes.slice(0, index)].join("\0"));
    return rotations.sort()[0] ?? "";
  }
}
