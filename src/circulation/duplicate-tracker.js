export class DuplicateTracker {
  constructor({ maxEntries = 50_000, now = Date.now } = {}) {
    if (!Number.isInteger(maxEntries) || maxEntries < 1) {
      throw new RangeError("maxEntries must be a positive integer");
    }
    this.maxEntries = maxEntries;
    this.now = now;
    this.entries = new Map();
    this.duplicates = 0;
  }

  has(packetId) {
    this.#expire();
    return this.entries.has(packetId);
  }

  mark(packetId, expiresAt) {
    if (typeof packetId !== "string" || packetId.length === 0) {
      throw new TypeError("packetId is required");
    }
    this.#expire();
    if (this.entries.has(packetId)) {
      this.duplicates += 1;
      return false;
    }
    if (this.entries.size >= this.maxEntries) {
      const oldest = this.entries.keys().next().value;
      if (oldest !== undefined) this.entries.delete(oldest);
    }
    this.entries.set(packetId, Number.isFinite(expiresAt) ? expiresAt : Infinity);
    return true;
  }

  get size() {
    this.#expire();
    return this.entries.size;
  }

  clear() {
    this.entries.clear();
  }

  #expire() {
    const now = this.now();
    for (const [packetId, expiresAt] of this.entries) {
      if (expiresAt <= now) this.entries.delete(packetId);
    }
  }
}
