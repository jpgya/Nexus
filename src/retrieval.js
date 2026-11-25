import { decodeReedSolomon } from "./encoder/reed-solomon.js";
import { sha256, validatePacketIntegrity } from "./integrity.js";

export async function retrieveObject(objectId, packets, {
  reachable = true,
  authenticator,
  now = Date.now()
} = {}) {
  if (!Array.isArray(packets)) throw new TypeError("packets must be an array");
  const unique = new Map();
  const objectPackets = packets.filter((packet) => packet?.objectId === objectId);

  for (const packet of objectPackets) {
    if (unique.has(packet.packetId)) continue;
    if (!(await validatePacketIntegrity(packet, now))) continue;
    if (packet.authentication?.scheme === "hmac-sha256") {
      if (!authenticator || !(await authenticator.verify(packet))) continue;
    } else if (packet.authentication?.scheme !== "none") {
      continue;
    }
    unique.set(packet.packetId, packet);
  }

  const accepted = [...unique.values()];
  if (accepted.length === 0 && !reachable) {
    return { status: "unavailable", packetsFound: 0, packetsNeeded: null };
  }
  if (accepted.length === 0) {
    return { status: "insufficient", packetsFound: 0, packetsNeeded: null };
  }

  const reference = accepted[0];
  const match = /^rs-gf256-v1:(\d+):(\d+):(\d+)$/.exec(reference.encodingId);
  if (!match) {
    return { status: "insufficient", packetsFound: accepted.length, packetsNeeded: null };
  }
  const [, dataShardsText, parityShardsText, originalLengthText] = match;
  const dataShards = Number(dataShardsText);
  const totalShards = dataShards + Number(parityShardsText);
  const originalLength = Number(originalLengthText);
  const consistent = accepted.filter((packet) => packet.encodingId === reference.encodingId);
  const distinctSequences = new Set(consistent.map((packet) => packet.sequenceNumber));
  const packetsFound = distinctSequences.size;
  if (packetsFound < dataShards) {
    return {
      status: "insufficient",
      packetsFound,
      packetsNeeded: dataShards,
      reachable
    };
  }

  try {
    const data = decodeReedSolomon(
      consistent.map((packet) => ({ index: packet.sequenceNumber, data: packet.payload })),
      { dataShards, totalShards, originalLength }
    );
    if (await sha256(data) !== objectId) {
      return {
        status: "insufficient",
        packetsFound,
        packetsNeeded: dataShards,
        reachable,
        reason: "Reconstructed object hash did not match"
      };
    }
    return { status: "recovered", data, packetsFound, packetsNeeded: dataShards, reachable };
  } catch (error) {
    return {
      status: "insufficient",
      packetsFound,
      packetsNeeded: dataShards,
      reachable,
      reason: error.message
    };
  }
}
