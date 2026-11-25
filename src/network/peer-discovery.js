export class PeerDiscovery {
  async start(_onPeer) {
    throw new Error("PeerDiscovery.start must be implemented");
  }

  async stop() {
    throw new Error("PeerDiscovery.stop must be implemented");
  }
}

export class ManualPeerDiscovery extends PeerDiscovery {
  constructor() {
    super();
    this.onPeer = null;
  }

  async start(onPeer) {
    if (typeof onPeer !== "function") throw new TypeError("onPeer callback is required");
    this.onPeer = onPeer;
    return () => { this.onPeer = null; };
  }

  announce(peer) {
    if (!this.onPeer) throw new Error("Manual discovery has not been started");
    this.onPeer(peer);
  }

  async stop() {
    this.onPeer = null;
  }
}

export class ApplicationPeerDiscovery extends PeerDiscovery {
  constructor(provider) {
    super();
    if (!provider || typeof provider.start !== "function" || typeof provider.stop !== "function") {
      throw new TypeError("Application discovery provider must implement start and stop");
    }
    this.provider = provider;
  }

  start(onPeer) {
    return this.provider.start(onPeer);
  }

  stop() {
    return this.provider.stop();
  }
}

export class LanPeerDiscovery extends ApplicationPeerDiscovery {
  constructor(adapter) {
    if (!adapter || typeof adapter.start !== "function" || typeof adapter.stop !== "function") {
      throw new TypeError("LAN discovery requires an application-supplied adapter");
    }
    super(adapter);
  }
}
