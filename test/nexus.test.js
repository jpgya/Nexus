import test from "node:test";
import assert from "node:assert/strict";
import { encodeReedSolomon, decodeReedSolomon } from "../src/encoder/reed-solomon.js";
import { createPackets } from "../src/packet.js";
import { retrieveObject } from "../src/retrieval.js";
import { HmacAuthenticator, validatePacketIntegrity } from "../src/integrity.js";
import { NetworkSimulator } from "../src/simulator.js";

const bytes = (text) => new TextEncoder().encode(text);

test("Reed-Solomon recovers data from any distinct configured shard subset", () => {
  const input = bytes("recover across missing and reordered packets");
  const encoded = encodeReedSolomon(input, { dataShards: 4, parityShards: 3 });
  const recovered = decodeReedSolomon(
    [6, 2, 4, 0].map((index) => ({ index, data: encoded[index] })),
    { dataShards: 4, totalShards: 7, originalLength: input.length }
  );
  assert.deepEqual(recovered, input);
});

test("duplicate shard indexes do not count toward decode threshold", () => {
  const encoded = encodeReedSolomon(bytes("duplicate"), { dataShards: 3, parityShards: 2 });
  assert.throws(() => decodeReedSolomon(
    [0, 0, 0].map((index) => ({ index, data: encoded[index] })),
    { dataShards: 3, totalShards: 5, originalLength: 9 }
  ), /distinct shards/);
});

test("the 95-of-100 research configuration recovers with exactly five lost shards", () => {
  const input = bytes("95 of 100 distinct shards");
  const encoded = encodeReedSolomon(input, { dataShards: 95, parityShards: 5 });
  const recovered = decodeReedSolomon(
    encoded.slice(5).map((data, offset) => ({ index: offset + 5, data })),
    { dataShards: 95, totalShards: 100, originalLength: input.length }
  );
  assert.deepEqual(recovered, input);
});

test("packet validation and retrieval filter corrupt, duplicate, and expired packets", async () => {
  const input = bytes("secure reconstruction");
  const packets = await createPackets(input, { dataShards: 3, parityShards: 2 });
  assert.equal(await validatePacketIntegrity(packets[0]), true);
  const corrupted = { ...packets[4], payload: bytes("corrupt") };
  const result = await retrieveObject(packets[0].objectId, [
    packets[0], packets[0], packets[1], corrupted, packets[3]
  ]);
  assert.equal(result.status, "recovered");
  assert.deepEqual(result.data, input);
  assert.equal(result.packetsFound, 3);

  const missing = await retrieveObject(packets[0].objectId, packets.slice(0, 2));
  assert.equal(missing.status, "insufficient");
  assert.equal(missing.packetsNeeded, 3);
  const unreachable = await retrieveObject("missing", [], { reachable: false });
  assert.equal(unreachable.status, "unavailable");
});

test("HMAC-authenticated packets reject tampering and wrong keys", async () => {
  const signer = new HmacAuthenticator(new Uint8Array(32).fill(7));
  const other = new HmacAuthenticator(new Uint8Array(32).fill(8));
  const packets = await createPackets(bytes("authenticated"), {
    dataShards: 2, parityShards: 1, authenticator: signer
  });
  assert.equal((await retrieveObject(packets[0].objectId, packets, { authenticator: signer })).status, "recovered");
  assert.equal((await retrieveObject(packets[0].objectId, packets, { authenticator: other })).status, "insufficient");
  const malformed = {
    ...packets[0],
    authentication: { scheme: "hmac-sha256", signature: "not-a-signature" }
  };
  assert.equal((await retrieveObject(packets[0].objectId, [malformed])).status, "insufficient");
});

test("simulator circulates packets on independent routes and recovers after failure", async () => {
  const simulator = new NetworkSimulator({ random: () => 0.5 });
  for (const node of ["a", "b", "c", "d", "e", "f"]) simulator.addNode(node);
  simulator.partition([["a", "b", "c"], ["d", "e", "f"]]);
  const created = await simulator.addObject(bytes("survives one route"), {
    dataShards: 3, parityShards: 2
  });
  await simulator.step();
  const status = simulator.getStatus();
  assert.equal(status.routes.length, 2);
  assert.ok(status.metrics.forwarded > 0);
  simulator.setNodeOnline("a", false);
  assert.equal(simulator.getStatus().routes.length, 2);

  const partial = await simulator.retrieve(created.objectId, { fromNodeId: "b" });
  assert.equal(partial.status, "recovered");
  simulator.healPartitions();
  assert.equal(simulator.getStatus().routes.length, 1);
  const recovered = await simulator.retrieve(created.objectId, { fromNodeId: "b" });
  assert.equal(recovered.status, "recovered");
  assert.deepEqual(recovered.data, bytes("survives one route"));
});

test("expired packets cannot be retrieved and unavailable nodes are reported", async () => {
  const simulator = new NetworkSimulator();
  simulator.addNode("only");
  const object = await simulator.addObject(bytes("ephemeral"), {
    dataShards: 1, parityShards: 0, expiresInMs: 1
  });
  const expired = await simulator.retrieve(object.objectId, { now: Date.now() + 10 });
  assert.equal(expired.status, "insufficient");
  simulator.setNodeOnline("only", false);
  const unavailable = await simulator.retrieve(object.objectId, { fromNodeId: "only" });
  assert.equal(unavailable.status, "unavailable");
});
