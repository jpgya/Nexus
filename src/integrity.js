const encoder = new TextEncoder();

export function bytesToHex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function sha256(bytes) {
  if (!(bytes instanceof Uint8Array)) throw new TypeError("sha256 input must be a Uint8Array");
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return bytesToHex(new Uint8Array(digest));
}

export function packetSigningBytes(packet) {
  const metadata = {
    objectId: packet.objectId,
    packetId: packet.packetId,
    sequenceNumber: packet.sequenceNumber,
    encodingId: packet.encodingId,
    integrityHash: packet.integrityHash,
    expiresAt: packet.expiresAt,
    routingInfo: {
      routeEpoch: packet.routingInfo.routeEpoch,
      hopLimit: packet.routingInfo.hopLimit
    },
    payload: bytesToHex(packet.payload)
  };
  return encoder.encode(JSON.stringify(metadata));
}

export async function packetIntegrityHash(packet) {
  return sha256(packetSigningBytes({ ...packet, integrityHash: "" }));
}

export async function validatePacketIntegrity(packet, now = Date.now()) {
  if (!packet || typeof packet !== "object"
    || !(packet.payload instanceof Uint8Array)
    || typeof packet.objectId !== "string"
    || typeof packet.packetId !== "string"
    || !Number.isInteger(packet.sequenceNumber)
    || packet.sequenceNumber < 0
    || typeof packet.encodingId !== "string"
    || typeof packet.integrityHash !== "string"
    || !/^[0-9a-f]{64}$/.test(packet.integrityHash)
    || !Number.isFinite(packet.expiresAt)
    || packet.expiresAt <= now
    || !packet.routingInfo
    || !Number.isInteger(packet.routingInfo.routeEpoch)
    || !Number.isInteger(packet.routingInfo.hopLimit)
    || packet.routingInfo.hopLimit < 0) {
    return false;
  }
  return (await packetIntegrityHash(packet)) === packet.integrityHash;
}

export class HmacAuthenticator {
  constructor(key) {
    if (!(key instanceof Uint8Array) || key.length < 32) {
      throw new RangeError("HMAC keys must contain at least 32 bytes");
    }
    this.key = key;
  }

  async sign(packet) {
    const key = await globalThis.crypto.subtle.importKey(
      "raw", this.key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const signature = await globalThis.crypto.subtle.sign("HMAC", key, packetSigningBytes(packet));
    return bytesToHex(new Uint8Array(signature));
  }

  async verify(packet) {
    if (typeof packet.authentication?.signature !== "string"
      || !/^[0-9a-f]{64}$/.test(packet.authentication.signature)) {
      return false;
    }
    const key = await globalThis.crypto.subtle.importKey(
      "raw", this.key, { name: "HMAC", hash: "SHA-256" }, false, ["verify"]
    );
    const signature = Uint8Array.from(packet.authentication.signature.match(/.{2}/g), (byte) =>
      Number.parseInt(byte, 16));
    return globalThis.crypto.subtle.verify("HMAC", key, signature, packetSigningBytes(packet));
  }
}
