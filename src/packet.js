import { packetIntegrityHash } from "./integrity.js";

export async function createPackets(data, {
  dataShards,
  parityShards,
  expiresInMs = 24 * 60 * 60 * 1000,
  authenticator,
  routeEpoch = 0
}) {
  if (!(data instanceof Uint8Array)) throw new TypeError("data must be a Uint8Array");
  if (!Number.isFinite(expiresInMs) || expiresInMs <= 0) {
    throw new RangeError("expiresInMs must be a positive finite number");
  }
  const { encodeReedSolomon } = await import("./encoder/reed-solomon.js");
  const { sha256 } = await import("./integrity.js");
  const shards = encodeReedSolomon(data, { dataShards, parityShards });
  const objectId = await sha256(data);
  const encodingId = `rs-gf256-v1:${dataShards}:${parityShards}:${data.length}`;
  const expiresAt = Date.now() + expiresInMs;

  return Promise.all(shards.map(async (payload, sequenceNumber) => {
    const packet = {
      objectId,
      packetId: `${objectId}:${sequenceNumber}`,
      sequenceNumber,
      encodingId,
      integrityHash: "",
      expiresAt,
      authentication: { scheme: "none" },
      routingInfo: { routeEpoch, hopLimit: 64 },
      payload
    };
    packet.integrityHash = await packetIntegrityHash(packet);
    if (authenticator) {
      packet.authentication = {
        scheme: "hmac-sha256",
        signature: await authenticator.sign(packet)
      };
    }
    return packet;
  }));
}
