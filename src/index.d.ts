export interface EncodingOptions {
  dataShards: number;
  parityShards: number;
}

export interface PacketOptions extends EncodingOptions {
  expiresInMs?: number;
  authenticator?: PacketAuthenticator;
  routeEpoch?: number;
}

export interface PacketAuthenticator {
  sign(packet: Packet): Promise<string>;
  verify(packet: Packet): Promise<boolean>;
}

export interface Packet {
  objectId: string;
  packetId: string;
  sequenceNumber: number;
  encodingId: string;
  integrityHash: string;
  expiresAt: number;
  authentication:
    | { scheme: "none" }
    | { scheme: "hmac-sha256"; signature: string }
    | { scheme: string; [key: string]: unknown };
  routingInfo: { routeEpoch: number; hopLimit: number };
  payload: Uint8Array;
}

export type RetrievalResult =
  | {
      status: "recovered";
      data: Uint8Array;
      packetsFound: number;
      packetsNeeded: number;
      reachable: boolean;
      measured?: { durationMs: number; retrievalSuccessRate: number; averageRetrievalMs: number };
      peerErrors?: string[];
    }
  | {
      status: "insufficient" | "unavailable";
      packetsFound: number;
      packetsNeeded: number | null;
      reachable?: boolean;
      measured?: { durationMs: number; retrievalSuccessRate: number; averageRetrievalMs: number };
      reason?: string;
      peerErrors?: string[];
    };

export function encodeReedSolomon(
  data: Uint8Array,
  options: EncodingOptions
): Uint8Array[];
export function decodeReedSolomon(
  shards: Array<{ index: number; data: Uint8Array }>,
  options: { dataShards: number; totalShards: number; originalLength: number }
): Uint8Array;
export function createPackets(data: Uint8Array, options: PacketOptions): Promise<Packet[]>;
export function retrieveObject(
  objectId: string,
  packets: Packet[],
  options?: { reachable?: boolean; authenticator?: PacketAuthenticator; now?: number }
): Promise<RetrievalResult>;
export function validatePacketIntegrity(packet: Packet, now?: number): Promise<boolean>;

export class HmacAuthenticator implements PacketAuthenticator {
  constructor(key: Uint8Array);
  sign(packet: Packet): Promise<string>;
  verify(packet: Packet): Promise<boolean>;
}

export class Transport {
  send(peerId: string, message: unknown): Promise<void>;
  request(peerId: string, message: unknown, options?: unknown): Promise<unknown>;
  subscribe(handler: (peerId: string, message: unknown) => void): () => void;
  subscribeStatus?(handler: (peers: WebRtcPeerStatus[]) => void): () => void;
  close(): void;
}

export class TransferController {
  constructor(transport: Transport, options?: {
    maxMessageBytes?: number;
    maxQueuedBytesPerPeer?: number;
    maxRetries?: number;
    retryDelayMs?: number;
  });
  send(peerId: string, message: unknown): Promise<number>;
  getStatus(): {
    metrics: { sentBytes: number; sentMessages: number; failedMessages: number; retries: number };
    queuedBytes: Record<string, number>;
  };
}

export interface DiscoveredPeer {
  peerId: string;
  [key: string]: unknown;
}

export class PeerDiscovery {
  start(onPeer: (peer: string | DiscoveredPeer) => void): Promise<unknown> | unknown;
  stop(): Promise<void> | void;
}

export class ManualPeerDiscovery extends PeerDiscovery {
  announce(peer: string | DiscoveredPeer): void;
}

export class ApplicationPeerDiscovery extends PeerDiscovery {
  constructor(provider: PeerDiscovery);
}

export class LanPeerDiscovery extends ApplicationPeerDiscovery {
  constructor(adapter: PeerDiscovery);
}

export interface HybridRoute {
  id: string;
  generation: string;
  nodes: string[];
  edges: string[];
}

export class HybridRouteManager {
  constructor(options?: { maxRoutes?: number; maxRouteLength?: number; minRouteLength?: number });
  readonly generation: number;
  readonly topologyId: string;
  setTopology(topology: Map<string, Set<string> | string[]>): number;
  addLink(left: string, right: string): boolean;
  removeLink(left: string, right: string): boolean;
  getRouteId(nodes: string[], topologyId?: string): string;
  isValidRoute(nodes: string[], topologyId?: string, routeId?: string): boolean;
  getRoutes(localNodeId: string, options?: { maxRoutes?: number }): HybridRoute[];
  getTopology(): Map<string, Set<string>>;
}

export class DuplicateTracker {
  constructor(options?: { maxEntries?: number; now?: () => number });
  readonly size: number;
  readonly duplicates: number;
  has(packetId: string): boolean;
  mark(packetId: string, expiresAt?: number): boolean;
  clear(): void;
}

export class CirculationEngine {
  constructor(options: {
    nodeId: string;
    routeManager: HybridRouteManager;
    send(peerId: string, message: unknown): Promise<number | void>;
    intervalMs?: number;
    maxCircuits?: number;
    maxRelayHops?: number;
    now?: () => number;
    onMetric?: (event: unknown) => void;
  });
  start(): boolean;
  stop(): boolean;
  register(packet: Packet): boolean;
  receive(peerId: string, envelope: unknown): Promise<boolean>;
  tick(): Promise<void>;
  isStalled(thresholdMs?: number): boolean;
  getStatus(): {
    running: boolean;
    circulatingPackets: number;
    routeCount: number;
    lastProgressAt: number | null;
    stalled: boolean;
    metrics: {
      laps: number;
      forwarded: number;
      bytes: number;
      startedAt: number | null;
      lastProgressAt: number | null;
      stoppedRoutes: number;
    };
  };
  close(): void;
}

export interface WebRtcPeerStatus {
  id: string;
  connectionState: string;
  iceConnectionState: string;
  dataChannelState: string;
}

export class WebRtcTransport extends Transport {
  constructor(options?: {
    rtcConfiguration?: RTCConfiguration;
    RTCPeerConnection?: typeof RTCPeerConnection;
    maxMessageBytes?: number;
    requestTimeoutMs?: number;
    iceGatheringTimeoutMs?: number;
    disconnectGraceMs?: number;
    dataChannelLabel?: string;
  });
  createOffer(peerId: string): Promise<string>;
  acceptOffer(peerId: string, offerSdp: string): Promise<string>;
  acceptAnswer(peerId: string, answerSdp: string): Promise<void>;
  send(peerId: string | "*", message: unknown): Promise<void>;
  request(peerId: string, message: unknown, options?: { timeoutMs?: number }): Promise<unknown>;
  subscribe(handler: (peerId: string, message: unknown) => unknown | Promise<unknown>): () => void;
  getPeers(): WebRtcPeerStatus[];
  closePeer(peerId: string): void;
  close(): void;
}

export class NexusNode {
  constructor(transport: Transport, options?: {
    nodeId?: string;
    maxPackets?: number;
    maxMemoryBytes?: number;
    maxPacketBytes?: number;
    maxPeers?: number;
    maxTopologyNodes?: number;
    maxRetrievePackets?: number;
    maxRetrieveBytes?: number;
    maxRelayHops?: number;
    maxRoutes?: number;
    maxSearchHops?: number;
    maxSearchPeers?: number;
    circulationIntervalMs?: number;
    maxCircuits?: number;
    maxQueuedBytesPerPeer?: number;
    maxMessageBytes?: number;
    maxRetries?: number;
    autoStartCirculation?: boolean;
    discovery?: PeerDiscovery;
    authenticator?: PacketAuthenticator;
  });
  readonly nodeId: string;
  publish(data: Uint8Array, options: PacketOptions): Promise<{
    objectId: string;
    packetsCreated: number;
  }>;
  retrieve(
    objectId: string,
    options?: {
      timeoutMs?: number;
      authenticator?: PacketAuthenticator;
      now?: number;
      maxHops?: number;
      maxPeers?: number;
    }
  ): Promise<RetrievalResult>;
  addPeer(peerId: string): void;
  removePeer(peerId: string): void;
  syncPeer(peerId: string): Promise<{ packetsSent: number }>;
  startCirculation(): boolean;
  stopCirculation(): boolean;
  startDiscovery(onPeerDiscovered?: (peer: string | DiscoveredPeer) => void): Promise<unknown>;
  stopDiscovery(): Promise<void>;
  subscribe(listener: (packet: Packet) => void): () => void;
  getStatus(): {
    nodeId: string;
    peers: string[];
    peerCount: number;
    configuredPeerCount: number;
    discoveredPeerCount: number;
    packetCount: number;
    packetBytes: number;
    payloadMemory: {
      retainedPayloadBytes: number;
      totalHeapBytes: null;
      note: string;
    };
    routes: HybridRoute[];
    routeCount: number;
    circulation: ReturnType<CirculationEngine["getStatus"]>;
    transfer: ReturnType<TransferController["getStatus"]>;
    measurementSource: "runtime";
    metrics: Record<string, number | boolean>;
  };
  close(): void;
}

export class NetworkSimulator {
  constructor(options?: {
    maxPacketsPerNode?: number;
    maxMemoryBytesPerNode?: number;
    random?: () => number;
  });
  addNode(id: string): void;
  setNodeOnline(id: string, online: boolean): void;
  partition(groups: string[][]): void;
  healPartitions(): void;
  reconfigureRoutes(): Array<{ id: string; nodes: string[] }>;
  addObject(data: Uint8Array, options: PacketOptions): Promise<{
    objectId: string;
    packetsCreated: number;
  }>;
  step(options?: { lossRate?: number; duplicateRate?: number }): Promise<{
    forwarded: number;
    dropped: number;
    duplicates: number;
    routes: number;
  }>;
  retrieve(
    objectId: string,
    options?: { fromNodeId?: string; authenticator?: PacketAuthenticator; now?: number }
  ): Promise<RetrievalResult>;
  getStatus(): {
    nodes: Array<{ id: string; online: boolean; packetCount: number; packetBytes: number }>;
    routes: Array<{ id: string; nodes: string[] }>;
    routeEpoch: number;
    metrics: Record<string, number>;
    measurementSource: "simulation";
    memoryBytes: number;
  };
}
