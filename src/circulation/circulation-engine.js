export class CirculationEngine {
  constructor({
    nodeId,
    routeManager,
    send,
    intervalMs = 5_000,
    maxCircuits = 10_000,
    maxRelayHops = 32,
    now = Date.now,
    onMetric = () => {}
  }) {
    if (typeof nodeId !== "string" || !nodeId
      || !routeManager || typeof routeManager.getRoutes !== "function"
      || typeof send !== "function") {
      throw new TypeError("Circulation engine requires nodeId, routeManager, and send");
    }
    if (!Number.isFinite(intervalMs) || intervalMs < 100
      || !Number.isInteger(maxCircuits) || maxCircuits < 1
      || !Number.isInteger(maxRelayHops) || maxRelayHops < 1) {
      throw new RangeError("Invalid circulation limits");
    }
    this.nodeId = nodeId;
    this.routeManager = routeManager;
    this.send = send;
    this.intervalMs = intervalMs;
    this.maxCircuits = maxCircuits;
    this.maxRelayHops = maxRelayHops;
    this.now = now;
    this.onMetric = onMetric;
    this.timer = null;
    this.packets = new Map();
    this.circuits = new Map();
    this.seenFrames = new Map();
    this.originLaps = new Map();
    this.lastProgress = now();
    this.previousRouteCount = 0;
    this.metrics = {
      laps: 0,
      forwarded: 0,
      bytes: 0,
      startedAt: null,
      lastProgressAt: null,
      stoppedRoutes: 0
    };
  }

  start() {
    if (this.timer) return false;
    this.metrics.startedAt = this.now();
    this.timer = setInterval(() => {
      this.tick().catch((error) => this.onMetric({ type: "error", error }));
    }, this.intervalMs);
    this.timer.unref?.();
    this.tick().catch((error) => this.onMetric({ type: "error", error }));
    return true;
  }

  stop() {
    if (!this.timer) return false;
    clearInterval(this.timer);
    this.timer = null;
    return true;
  }

  register(packet) {
    if (!packet || typeof packet.packetId !== "string" || !Number.isFinite(packet.expiresAt)) {
      throw new TypeError("Invalid packet for circulation");
    }
    this.#expire();
    if (this.packets.has(packet.packetId)) return false;
    if (this.packets.size >= this.maxCircuits) {
      const oldest = this.packets.keys().next().value;
      if (oldest !== undefined) {
        this.packets.delete(oldest);
        this.#clearPacketState(oldest);
      }
    }
    this.packets.set(packet.packetId, packet);
    return true;
  }

  unregister(packetId) {
    const existed = this.packets.delete(packetId);
    this.#clearPacketState(packetId);
    return existed;
  }

  async receive(peerId, envelope) {
    if (!this.isValidFrame(peerId, envelope)) return false;
    const { packet, routeId, routeGeneration, nodes, position, lap, hops } = envelope ?? {};
    this.register(packet);
    const circuitKey = `${packet.packetId}:${routeId}`;
    const frameKey = `${circuitKey}:${lap}:${position}`;
    if (this.seenFrames.has(frameKey)) return false;
    if (this.seenFrames.size >= this.maxCircuits * this.maxRelayHops) {
      this.seenFrames.delete(this.seenFrames.keys().next().value);
    }
    this.seenFrames.set(frameKey, packet.expiresAt);
    if (position === 0) this.circuits.set(circuitKey, Math.max(this.circuits.get(circuitKey) ?? 0, lap));
    if (position === 0 && lap > 0) {
      this.metrics.laps += 1;
      this.lastProgress = this.now();
      this.metrics.lastProgressAt = this.lastProgress;
    }
    if (hops >= this.maxRelayHops) return true;
    const nextPosition = (position + 1) % nodes.length;
    const nextPeer = nodes[nextPosition];
    const nextLap = nextPosition === 0 ? lap + 1 : lap;
    if (nextLap >= this.maxCircuits) return true;
    await this.#forward(nextPeer, {
      packet,
      routeId,
      routeGeneration,
      nodes,
      position: nextPosition,
      lap: nextLap,
      hops: hops + 1
    });
    return true;
  }

  isValidFrame(peerId, envelope) {
    const { packet, routeId, routeGeneration, nodes, position, lap, hops } = envelope ?? {};
    if (!packet || typeof packet.packetId !== "string" || typeof routeId !== "string"
      || !Array.isArray(nodes) || !Number.isInteger(position) || position < 0
      || position >= nodes.length || !Number.isInteger(lap) || lap < 0
      || !Number.isInteger(hops) || hops < 1 || hops > this.maxRelayHops
      || nodes[position] !== this.nodeId
      || nodes[(position + nodes.length - 1) % nodes.length] !== peerId
      || nodes.length < 3 || nodes.length > 16
      || !this.routeManager.isValidRoute(nodes, routeGeneration, routeId)) return false;
    return true;
  }

  async tick() {
    this.#expire();
    const routes = this.routeManager.getRoutes(this.nodeId);
    if (routes.length === 0 && this.previousRouteCount > 0) this.metrics.stoppedRoutes += 1;
    this.previousRouteCount = routes.length;
    for (const route of routes) {
      if (route.nodes[0] !== this.nodeId
        || this.nodeId !== [...route.nodes].sort()[0]) continue;
      const peerId = route.nodes[1];
      for (const packet of this.packets.values()) {
        const key = `${packet.packetId}:${route.id}`;
        const lap = this.circuits.get(key) ?? 0;
        if (lap >= this.maxCircuits) continue;
        const lastStartedLap = this.originLaps.get(key) ?? -1;
        if (lap <= lastStartedLap) continue;
        this.originLaps.set(key, lap);
        const frameKey = `${key}:${lap}:1`;
        this.seenFrames.set(frameKey, packet.expiresAt);
        await this.#forward(peerId, {
          packet,
          routeId: route.id,
          routeGeneration: route.generation,
          nodes: route.nodes,
          position: 1,
          lap,
          hops: 1
        });
      }
    }
  }

  isStalled(thresholdMs = this.intervalMs * 3) {
    return this.packets.size > 0 && this.now() - this.lastProgress >= thresholdMs;
  }

  getStatus() {
    this.#expire();
    return {
      running: Boolean(this.timer),
      circulatingPackets: this.packets.size,
      routeCount: this.routeManager.getRoutes(this.nodeId).length,
      lastProgressAt: this.metrics.lastProgressAt,
      stalled: this.isStalled(),
      metrics: { ...this.metrics }
    };
  }

  close() {
    this.stop();
    this.packets.clear();
    this.circuits.clear();
    this.seenFrames.clear();
    this.originLaps.clear();
  }

  async #forward(peerId, envelope) {
    try {
      const bytes = await this.send(peerId, { type: "circulate", envelope });
      this.metrics.forwarded += 1;
      this.metrics.bytes += Number.isFinite(bytes) ? bytes : 0;
      this.onMetric({ type: "forwarded", peerId, packetId: envelope.packet.packetId });
    } catch (error) {
      if (envelope.position === 1) {
        const key = `${envelope.packet.packetId}:${envelope.routeId}`;
        this.originLaps.delete(key);
        this.seenFrames.delete(`${key}:${envelope.lap}:${envelope.position}`);
      }
      this.onMetric({ type: "route-send-failed", peerId, error });
    }
  }

  #expire(now = this.now()) {
    for (const [packetId, packet] of this.packets) {
      if (packet.expiresAt <= now) {
        this.packets.delete(packetId);
        this.#clearPacketState(packetId);
      }
    }
  }

  #clearPacketState(packetId) {
    for (const key of this.circuits.keys()) {
      if (key.startsWith(`${packetId}:`)) this.circuits.delete(key);
    }
    for (const key of this.seenFrames.keys()) {
      if (key.startsWith(`${packetId}:`)) this.seenFrames.delete(key);
    }
    for (const key of this.originLaps.keys()) {
      if (key.startsWith(`${packetId}:`)) this.originLaps.delete(key);
    }
  }
}
