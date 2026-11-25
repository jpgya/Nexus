# NEXUS circulation library

NEXUS is an experimental, serverless library for circulating erasure-coded packets among peers. It is not a storage service and does not promise that data survives when all copies and volatile state disappear.

## What is implemented

- Systematic Reed-Solomon erasure coding over GF(256), configurable up to 255 total shards.
- Packet identity, sequence, encoding metadata, expiry, SHA-256 integrity hashes, and routing hints.
- Optional HMAC-SHA-256 packet authentication.
- Recovery that filters invalid/expired packets and deduplicates packet IDs and shard sequence numbers.
- A common hybrid route manager used by the simulator and runtime nodes, including edge-disjoint cycles and link-down reconfiguration.
- A bounded circulation engine, packet duplicate tracker, per-peer transfer controller, distributed retrieval with request-loop suppression, and runtime/simulation metrics.
- Swappable manual, application-provided, and LAN-discovery adapter interfaces.
- A browser WebRTC data-channel transport with manual offer/answer signaling and bounded JSON frames.
- `NexusNode` for packet validation, deduplication, bounded-hop peer forwarding, and remote retrieval.
- An application-owned `Transport` interface; NEXUS starts no server and selects no signaling service.

## Quick start

```js
import { NetworkSimulator } from "@nexus/circulation";

const network = new NetworkSimulator();
for (const id of ["peer-a", "peer-b", "peer-c"]) network.addNode(id);

const { objectId } = await network.addObject(
  new TextEncoder().encode("hello NEXUS"),
  { dataShards: 3, parityShards: 2, expiresInMs: 60_000 }
);
await network.step({ lossRate: 0.01 });
const result = await network.retrieve(objectId, { fromNodeId: "peer-a" });

if (result.status === "recovered") {
  console.log(new TextDecoder().decode(result.data));
} else {
  console.log(result.status, result.packetsFound, result.packetsNeeded);
}
```

The default experiment in the brief (95 data shards plus 5 parity shards) can be selected with `{ dataShards: 95, parityShards: 5 }`. The data-shard threshold is the number of **distinct** sequence numbers, not packet copies.

## API and packet shape

`createPackets(data, options)` creates independently verifiable packet objects. Each includes `objectId`, `packetId`, `sequenceNumber`, `encodingId`, `integrityHash`, `expiresAt`, `authentication`, `routingInfo`, and a `Uint8Array` payload. `retrieveObject(objectId, packets, options)` returns `recovered`, `insufficient`, or `unavailable`; only `recovered` includes the reconstructed bytes. `reachable` distinguishes a known disconnected retrieval scope from reachable peers that do not currently have enough valid shards.

`NetworkSimulator` exposes `addNode`, `setNodeOnline`, `partition`, `healPartitions`, `reconfigureRoutes`, `addObject`, `step`, `retrieve`, and `getStatus`. Its metrics are counters from simulated operations, not measurements of a real network.

`Transport` is an application implementation point (`send`, `request`, `subscribe`, `close`). Peer discovery, signaling exchange, authorization, and NAT traversal remain application concerns. WebRTC does not eliminate the need for signaling; applications may supply out-of-band signaling or manual offer/answer exchange. `HybridRouteManager` is shared by the simulator and live nodes; live routes are calculated from bounded topology gossip over configured links. Link changes propagate as versioned topology updates, and circuits use a fixed route, packet/route/circuit deduplication, expiry, a hop limit, and a configurable circuit budget rather than unbounded peer broadcasts.

## WebRTC peer and debug page

`WebRtcTransport` implements the transport interface with browser `RTCPeerConnection` data channels. `NexusNode` builds packet forwarding and Object ID retrieval on that transport:

```js
import { NexusNode, WebRtcTransport } from "@nexus/circulation";

const transport = new WebRtcTransport({
  rtcConfiguration: { iceServers: [{ urls: "stun:your-stun.example:3478" }] }
});
const node = new NexusNode(transport, { nodeId: "peer-a" });

// Exchange offer/answer strings out of band with the other application.
node.addPeer("peer-b");
const offerSdp = await transport.createOffer("peer-b");
// Send offerSdp to peer-b. On peer-b:
// const answerSdp = await peerBTransport.acceptOffer("peer-a", offerSdp);
// On peer-a: await transport.acceptAnswer("peer-b", answerSdp);

const published = await node.publish(new TextEncoder().encode("hello"), {
  dataShards: 3, parityShards: 2
});
const result = await node.retrieve(published.objectId);
```

In the two-peer example, peer identifiers are local labels and must match the opposite side's label for each connection. `createOffer` and `acceptOffer` wait for ICE gathering to finish, producing a single SDP string suitable for copying; this trades setup latency for simple manual signaling. For robust Internet connectivity, the embedding application must provide suitable STUN/TURN infrastructure and signaling/discovery. A STUN server assists ICE but is not a NEXUS backend; TURN relays traffic and is not direct peer-to-peer. The library does not configure either service by default.

Run the browser debug page by serving this project directory as static files, for example with `python3 -m http.server 8000`, then open `http://localhost:8000/examples/debug.html` in two tabs. The static file server only serves the demo assets; it is not part of NEXUS and does not broker connections. Copy the generated Offer and Answer between the tabs, wait for the data channel to open, then publish and retrieve an object. The page shows actual browser WebRTC connection states and library packet counters. Browser support, firewall/NAT policy, and ICE configuration determine whether a particular pair can connect.

`NexusNode` starts circulation by default when it has packets; use `startCirculation()` and `stopCirculation()` to control it explicitly, or set `autoStartCirculation: false`. Each circulating copy follows one selected closed route at a time; ordinary first-delivery relay is bounded to at most `maxRoutes` peers. `maxCircuits` caps completed laps per packet/route, and `expiresInMs` still limits packet lifetime. `maxMemoryBytes` and `maxPacketBytes` bound each node's retained packet payload and shard size. The `getStatus()` data is runtime-observed; serialized communication bytes are estimates from encoded message sizes, and `payloadMemory.retainedPayloadBytes` is a lower bound rather than full JavaScript heap usage. Circulation is best-effort and cannot be guaranteed during a partition, peer outage, or exhausted route budget.

Retrieval requests have a shared request ID, visited-node list, configurable `maxHops` (0-8) and per-node `maxPeers` (1-32), in-flight query deduplication, and bounded paginated responses. The requester stops querying further peers after it has enough distinct sequence numbers to attempt recovery. A recovered result includes elapsed time and measured success-rate/average-duration counters. Requests can still time out or return insufficient shards.

`PeerDiscovery`, `ManualPeerDiscovery`, `ApplicationPeerDiscovery`, and `LanPeerDiscovery` provide replaceable discovery interfaces. `LanPeerDiscovery` is deliberately only an adapter: users must provide their own LAN discovery mechanism (for example, multicast DNS/UDP on a native host). Browsers cannot generally perform arbitrary LAN multicast discovery. Discovery reports candidates; it does not perform WebRTC signaling or automatically establish a connection.

## Security boundaries

SHA-256 detects accidental or malicious modification only if the expected object ID is trusted. HMAC provides authenticity only among peers that safely share the same secret; it is not public-key identity, encryption, or non-repudiation. Key distribution, peer reputation, abuse quotas, and encrypted transport are the embedding application's responsibility. Unknown authentication schemes are rejected during retrieval.

## Physical and availability limits

Circulation requires active nodes, working links, and available RAM or other physical state to hold each packet. This implementation keeps packet state in process memory; node shutdown loses that node's copies. Expiration, partitions, loss, resource limits, and exhausted redundancy can all make retrieval impossible. Signal transmission is subject to attenuation, noise, delay, and loss; regenerating information requires surviving physical state. Persistent information retention without any storage medium is an unresolved research problem, not a feature of NEXUS.

## Development

Requires Node.js 20 or newer. Run `npm test` for unit and multi-node integration tests and `npm run check` for syntax checks. The simulator reports `measurementSource: "simulation"`; live nodes report `"runtime"`. Simulator results are not measurements of physical networks.

## v0.2 WebRTC verification

1. Serve the static debug page and open two browser contexts (same LAN or two devices); enter reciprocal node IDs and initialize.
2. Copy the Offer to the answerer, then copy its Answer back to the offerer. Verify both data channels show `open`.
3. Publish a payload on one side, copy the Object ID, and retrieve it on the other side. The page logs individual received shard sequence numbers and the decoded payload.
4. Close a peer tab or its data channel; WebRTC failure/disconnection is observed (a transient `disconnected` state has a grace period) and the live route plan is recomputed. Reconnect by exchanging a fresh Offer/Answer and adding the peer again.
5. To observe reconfiguration with more than two nodes, connect a ring or mesh of peers. A single pair cannot provide multiple independent cycles. Without a configured STUN/TURN service, manual SDP signaling does not guarantee connectivity across separate NATs.

RAM, browser/network buffers, and in-flight DataChannel data remain physical temporary storage. NEXUS does not persist data after processes close, guarantee delivery, or provide encryption, reputation scoring, or a built-in LAN discovery daemon.
c