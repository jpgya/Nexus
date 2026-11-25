import { Transport } from "./transport.js";

function bytesToBase64(bytes) {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}

function toWire(value) {
  if (value instanceof Uint8Array) {
    return { __nexusType: "Uint8Array", base64: bytesToBase64(value) };
  }
  if (Array.isArray(value)) return value.map(toWire);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, toWire(item)]));
  }
  return value;
}

function fromWire(value) {
  if (Array.isArray(value)) return value.map(fromWire);
  if (value && typeof value === "object") {
    if (value.__nexusType === "Uint8Array" && typeof value.base64 === "string") {
      const binary = atob(value.base64);
      return Uint8Array.from(binary, (character) => character.charCodeAt(0));
    }
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, fromWire(item)]));
  }
  return value;
}

function waitForIceGathering(peerConnection, timeoutMs) {
  if (peerConnection.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out while gathering ICE candidates"));
    }, timeoutMs);
    const onStateChange = () => {
      if (peerConnection.iceGatheringState === "complete") {
        cleanup();
        resolve();
      }
    };
    const cleanup = () => {
      clearTimeout(timeout);
      peerConnection.removeEventListener("icegatheringstatechange", onStateChange);
    };
    peerConnection.addEventListener("icegatheringstatechange", onStateChange);
    onStateChange();
  });
}

export class WebRtcTransport extends Transport {
  constructor({
    rtcConfiguration = {},
    RTCPeerConnection: PeerConnection = globalThis.RTCPeerConnection,
    maxMessageBytes = 1_000_000,
    requestTimeoutMs = 10_000,
    iceGatheringTimeoutMs = 30_000,
    disconnectGraceMs = 5_000,
    dataChannelLabel = "nexus"
  } = {}) {
    super();
    if (typeof PeerConnection !== "function") {
      throw new Error("WebRTC is not available; provide an RTCPeerConnection implementation");
    }
    if (!Number.isInteger(maxMessageBytes) || maxMessageBytes < 1) {
      throw new RangeError("maxMessageBytes must be a positive integer");
    }
    if (!Number.isFinite(requestTimeoutMs) || requestTimeoutMs <= 0
      || !Number.isFinite(iceGatheringTimeoutMs) || iceGatheringTimeoutMs <= 0
      || !Number.isFinite(disconnectGraceMs) || disconnectGraceMs < 0) {
      throw new RangeError("Timeouts must be positive finite numbers");
    }
    this.PeerConnection = PeerConnection;
    this.rtcConfiguration = rtcConfiguration;
    this.maxMessageBytes = maxMessageBytes;
    this.requestTimeoutMs = requestTimeoutMs;
    this.iceGatheringTimeoutMs = iceGatheringTimeoutMs;
    this.disconnectGraceMs = disconnectGraceMs;
    this.dataChannelLabel = dataChannelLabel;
    this.peers = new Map();
    this.handlers = new Set();
    this.statusHandlers = new Set();
    this.pendingRequests = new Map();
    this.nextRequestId = 0;
    this.closed = false;
  }

  async createOffer(peerId) {
    const peer = this.#createPeer(peerId);
    try {
      peer.channel = peer.connection.createDataChannel(this.dataChannelLabel, { ordered: true });
      this.#bindChannel(peer, peer.channel);
      const offer = await peer.connection.createOffer();
      await peer.connection.setLocalDescription(offer);
      await waitForIceGathering(peer.connection, this.iceGatheringTimeoutMs);
      return peer.connection.localDescription.sdp;
    } catch (error) {
      this.closePeer(peerId);
      throw error;
    }
  }

  async acceptOffer(peerId, offerSdp) {
    if (typeof offerSdp !== "string" || offerSdp.trim().length === 0) {
      throw new TypeError("offerSdp must be a non-empty SDP string");
    }
    const peer = this.#createPeer(peerId);
    peer.connection.addEventListener("datachannel", (event) => {
      peer.channel = event.channel;
      this.#bindChannel(peer, peer.channel);
    });
    try {
      await peer.connection.setRemoteDescription({ type: "offer", sdp: offerSdp });
      const answer = await peer.connection.createAnswer();
      await peer.connection.setLocalDescription(answer);
      await waitForIceGathering(peer.connection, this.iceGatheringTimeoutMs);
      return peer.connection.localDescription.sdp;
    } catch (error) {
      this.closePeer(peerId);
      throw error;
    }
  }

  async acceptAnswer(peerId, answerSdp) {
    const peer = this.#peer(peerId);
    if (typeof answerSdp !== "string" || answerSdp.trim().length === 0) {
      throw new TypeError("answerSdp must be a non-empty SDP string");
    }
    try {
      await peer.connection.setRemoteDescription({ type: "answer", sdp: answerSdp });
    } catch (error) {
      this.closePeer(peerId);
      throw error;
    }
  }

  async send(peerId, message) {
    const destinations = peerId === "*"
      ? [...this.peers.values()]
      : [this.#peer(peerId)];
    if (destinations.length === 0) throw new Error("No connected peers are available");
    const frame = JSON.stringify(toWire({ kind: "message", message }));
    this.#assertMessageSize(frame);
    await Promise.all(destinations.map((peer) => this.#sendFrame(peer, frame)));
  }

  async request(peerId, message, { timeoutMs = this.requestTimeoutMs } = {}) {
    const peer = this.#peer(peerId);
    const id = `${Date.now().toString(36)}-${(++this.nextRequestId).toString(36)}`;
    const frame = JSON.stringify(toWire({ kind: "request", id, message }));
    this.#assertMessageSize(frame);
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`Request to peer "${peerId}" timed out`));
      }, timeoutMs);
      this.pendingRequests.set(id, { resolve, reject, timeout, peerId });
      this.#sendFrame(peer, frame).catch((error) => {
        clearTimeout(timeout);
        this.pendingRequests.delete(id);
        reject(error);
      });
    });
  }

  subscribe(handler) {
    if (typeof handler !== "function") throw new TypeError("handler must be a function");
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  subscribeStatus(handler) {
    if (typeof handler !== "function") throw new TypeError("handler must be a function");
    this.statusHandlers.add(handler);
    handler(this.getPeers());
    return () => this.statusHandlers.delete(handler);
  }

  getPeers() {
    return [...this.peers.values()].map(({ id, connection, channel }) => ({
      id,
      connectionState: connection.connectionState,
      iceConnectionState: connection.iceConnectionState,
      dataChannelState: channel?.readyState ?? "not-created"
    }));
  }

  closePeer(peerId) {
    const peer = this.peers.get(peerId);
    if (!peer) return;
    peer.channel?.close();
    peer.connection.close();
    clearTimeout(peer.disconnectTimer);
    this.peers.delete(peerId);
    for (const [id, request] of this.pendingRequests) {
      if (request.peerId === peerId) {
        clearTimeout(request.timeout);
        this.pendingRequests.delete(id);
        request.reject(new Error(`Peer "${peerId}" disconnected`));
      }
    }
    this.#notifyStatus();
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    for (const peerId of this.peers.keys()) this.closePeer(peerId);
    for (const [id, request] of this.pendingRequests) {
      clearTimeout(request.timeout);
      this.pendingRequests.delete(id);
      request.reject(new Error("Transport closed"));
    }
    this.handlers.clear();
    this.statusHandlers.clear();
  }

  #createPeer(peerId) {
    if (this.closed) throw new Error("Transport is closed");
    if (typeof peerId !== "string" || peerId.length === 0 || peerId === "*") {
      throw new TypeError("peerId must be a non-empty peer identifier");
    }
    if (this.peers.has(peerId)) throw new Error(`Peer already exists: ${peerId}`);
    const connection = new this.PeerConnection(this.rtcConfiguration);
    const peer = { id: peerId, connection, channel: null, disconnectTimer: null };
    this.peers.set(peerId, peer);
    connection.addEventListener("connectionstatechange", () => {
      this.#handlePeerConnectionState(peer);
      this.#notifyStatus();
      if (connection.connectionState === "failed" || connection.connectionState === "closed") {
        this.closePeer(peerId);
      }
    });
    connection.addEventListener("iceconnectionstatechange", () => {
      this.#handlePeerConnectionState(peer, connection.iceConnectionState);
      this.#notifyStatus();
      if (connection.iceConnectionState === "failed" || connection.iceConnectionState === "closed") {
        this.closePeer(peerId);
      }
    });
    this.#notifyStatus();
    return peer;
  }

  #peer(peerId) {
    const peer = this.peers.get(peerId);
    if (!peer) throw new Error(`Unknown peer: ${peerId}`);
    if (peer.connection.connectionState === "failed" || peer.connection.connectionState === "closed") {
      throw new Error(`Peer "${peerId}" is not connected`);
    }
    return peer;
  }

  #bindChannel(peer, channel) {
    channel.binaryType = "arraybuffer";
    channel.addEventListener("message", (event) => {
      this.#receiveFrame(peer.id, event.data).catch((error) => {
        this.#dispatch(peer.id, { type: "transport-error", error: error.message }).catch(() => {});
      });
    });
    channel.addEventListener("close", () => {
      if (peer.channel === channel) peer.channel = null;
      this.#notifyStatus();
    });
    channel.addEventListener("open", () => this.#notifyStatus());
    this.#notifyStatus();
  }

  async #sendFrame(peer, frame) {
    this.#assertMessageSize(frame);
    const channel = peer.channel;
    if (!channel || channel.readyState !== "open") {
      throw new Error(`Data channel to peer "${peer.id}" is not open`);
    }
    const highWaterMark = 4 * this.maxMessageBytes;
    if (channel.bufferedAmount > highWaterMark) {
      await this.#waitForBuffer(peer, channel, highWaterMark / 2);
    }
    if (channel.readyState !== "open") throw new Error(`Data channel to peer "${peer.id}" is not open`);
    channel.send(frame);
  }

  async #waitForBuffer(peer, channel, lowWaterMark) {
    channel.bufferedAmountLowThreshold = lowWaterMark;
    if (channel.bufferedAmount <= lowWaterMark) return;
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error(`Data channel to peer "${peer.id}" remained congested`));
      }, this.requestTimeoutMs);
      const onLow = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error(`Data channel to peer "${peer.id}" closed while congested`));
      };
      const cleanup = () => {
        clearTimeout(timeout);
        channel.removeEventListener("bufferedamountlow", onLow);
        channel.removeEventListener("close", onClose);
      };
      channel.addEventListener("bufferedamountlow", onLow, { once: true });
      channel.addEventListener("close", onClose, { once: true });
      if (channel.bufferedAmount <= lowWaterMark) onLow();
    });
  }

  #notifyStatus() {
    const status = this.getPeers();
    for (const handler of this.statusHandlers) handler(status);
  }

  #handlePeerConnectionState(peer) {
    const disconnected = peer.connection.connectionState === "disconnected"
      || peer.connection.iceConnectionState === "disconnected";
    if (!disconnected) {
      clearTimeout(peer.disconnectTimer);
      peer.disconnectTimer = null;
      return;
    }
    if (peer.disconnectTimer) return;
    peer.disconnectTimer = setTimeout(() => {
      peer.disconnectTimer = null;
      if (peer.connection.connectionState === "disconnected"
        || peer.connection.iceConnectionState === "disconnected") this.closePeer(peer.id);
    }, this.disconnectGraceMs);
  }

  async #receiveFrame(peerId, data) {
    if (typeof data !== "string") throw new TypeError("Only text data-channel frames are supported");
    this.#assertMessageSize(data);
    const envelope = fromWire(JSON.parse(data));
    if (!envelope || typeof envelope !== "object") throw new TypeError("Malformed data-channel frame");
    if (envelope.kind === "response") {
      const pending = this.pendingRequests.get(envelope.id);
      if (!pending) return;
      clearTimeout(pending.timeout);
      this.pendingRequests.delete(envelope.id);
      if (envelope.error) pending.reject(new Error(envelope.error));
      else pending.resolve(envelope.message);
      return;
    }
    if (envelope.kind !== "message" && envelope.kind !== "request") {
      throw new TypeError("Unknown data-channel frame kind");
    }
    try {
      const result = await this.#dispatch(peerId, envelope.message);
      if (envelope.kind === "request") {
        await this.#sendFrame(this.#peer(peerId), JSON.stringify(toWire({
          kind: "response",
          id: envelope.id,
          message: result
        })));
      }
    } catch (error) {
      if (envelope.kind === "request") {
        await this.#sendFrame(this.#peer(peerId), JSON.stringify(toWire({
          kind: "response",
          id: envelope.id,
          error: error.message
        })));
      } else {
        throw error;
      }
    }
  }

  async #dispatch(peerId, message) {
    let result;
    for (const handler of this.handlers) {
      const current = await handler(peerId, message);
      if (current !== undefined) result = current;
    }
    return result;
  }

  #assertMessageSize(frame) {
    if (new TextEncoder().encode(frame).length > this.maxMessageBytes) {
      throw new RangeError(`Message exceeds ${this.maxMessageBytes} bytes`);
    }
  }
}
