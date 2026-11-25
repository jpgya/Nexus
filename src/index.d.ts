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
      peerErrors?: string[];
    }
  | {
      status: "insufficient" | "unavailable";
      packetsFound: number;
      packetsNeeded: number | null;
      reachable?: boolean;
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
    maxPacketBytes?: number;
    maxRetrievePackets?: number;
    maxRetrieveBytes?: number;
    maxRelayHops?: number;
    authenticator?: PacketAuthenticator;
  });
  readonly nodeId: string;
  publish(data: Uint8Array, options: PacketOptions): Promise<{
    objectId: string;
    packetsCreated: number;
  }>;
  retrieve(
    objectId: string,
    options?: { timeoutMs?: number; authenticator?: PacketAuthenticator; now?: number }
  ): Promise<RetrievalResult>;
  addPeer(peerId: string): void;
  removePeer(peerId: string): void;
  syncPeer(peerId: string): Promise<{ packetsSent: number }>;
  subscribe(listener: (packet: Packet) => void): () => void;
  getStatus(): {
    nodeId: string;
    peers: string[];
    packetCount: number;
    metrics: { accepted: number; rejected: number; relayed: number; retrievals: number };
  };
  close(): void;
}

export class NetworkSimulator {
  constructor(options?: {
    maxPacketsPerNode?: number;
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
    nodes: Array<{ id: string; online: boolean; packetCount: number }>;
    routes: Array<{ id: string; nodes: string[] }>;
    routeEpoch: number;
    metrics: { forwarded: number; dropped: number; duplicates: number; retrievals: number };
  };
}
