# NEXUS circulation library

NEXUS is an experimental, serverless library for circulating erasure-coded packets among peers. It is not a storage service and does not promise that data survives when all copies and volatile state disappear.

## What is implemented

- Systematic Reed-Solomon erasure coding over GF(256), configurable up to 255 total shards.
- Packet identity, sequence, encoding metadata, expiry, SHA-256 integrity hashes, and routing hints.
- Optional HMAC-SHA-256 packet authentication.
- Recovery that filters invalid/expired packets and deduplicates packet IDs and shard sequence numbers.
- A deterministic, in-process simulator with multiple partition routes, node outages, packet loss/duplication, route reconfiguration, and retrieval.
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

`Transport` is an application implementation point (`send`, `request`, `subscribe`, `close`). Peer discovery, signaling exchange, authorization, and NAT traversal remain application concerns. WebRTC does not eliminate the need for signaling; applications may supply out-of-band signaling or manual offer/answer exchange.

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

`NexusNode` forwards new, valid packets to its configured peers once, using a bounded hop count and packet-ID deduplication. Retrieval requests query each currently configured peer and combine distinct valid shards. These operations are best-effort: a connection loss or timeout can leave the result insufficient; no background peer discovery, automatic retry mesh, persistence, end-to-end encryption, or peer reputation system is implied.

## Security boundaries

SHA-256 detects accidental or malicious modification only if the expected object ID is trusted. HMAC provides authenticity only among peers that safely share the same secret; it is not public-key identity, encryption, or non-repudiation. Key distribution, peer reputation, abuse quotas, and encrypted transport are the embedding application's responsibility. Unknown authentication schemes are rejected during retrieval.

## Physical and availability limits

Circulation requires active nodes, working links, and available RAM or other physical state to hold each packet. This implementation keeps packet state in process memory; node shutdown loses that node's copies. Expiration, partitions, loss, resource limits, and exhausted redundancy can all make retrieval impossible. Signal transmission is subject to attenuation, noise, delay, and loss; regenerating information requires surviving physical state. Persistent information retention without any storage medium is an unresolved research problem, not a feature of NEXUS.

## Development

Requires Node.js 20 or newer. Run `npm test` for unit tests and `npm run check` for syntax checks. The simulator validates core behavior; the WebRTC adapter uses the browser's real data channel when used in a browser. Neither the simulator nor the debug page demonstrates global P2P availability or production security.
c