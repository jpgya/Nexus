export class TransferController {
  constructor(transport, {
    maxMessageBytes = 1_000_000,
    maxQueuedBytesPerPeer = 4_000_000,
    maxRetries = 1,
    retryDelayMs = 100
  } = {}) {
    if (!transport || typeof transport.send !== "function") {
      throw new TypeError("transport.send is required");
    }
    if (!Number.isInteger(maxMessageBytes) || maxMessageBytes < 1
      || !Number.isInteger(maxQueuedBytesPerPeer) || maxQueuedBytesPerPeer < maxMessageBytes
      || !Number.isInteger(maxRetries) || maxRetries < 0
      || !Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
      throw new RangeError("Invalid transfer resource limits");
    }
    this.transport = transport;
    this.maxMessageBytes = maxMessageBytes;
    this.maxQueuedBytesPerPeer = maxQueuedBytesPerPeer;
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;
    this.queues = new Map();
    this.metrics = { sentBytes: 0, sentMessages: 0, failedMessages: 0, retries: 0 };
  }

  async send(peerId, message) {
    const bytes = this.#estimateBytes(message);
    if (bytes > this.maxMessageBytes) {
      throw new RangeError(`Transfer exceeds ${this.maxMessageBytes} bytes`);
    }
    const state = this.queues.get(peerId) ?? { tail: Promise.resolve(), queuedBytes: 0 };
    if (state.queuedBytes + bytes > this.maxQueuedBytesPerPeer) {
      throw new RangeError(`Peer "${peerId}" transfer queue is full`);
    }
    state.queuedBytes += bytes;
    const sendOperation = state.tail.then(async () => {
      try {
        for (let attempt = 0; attempt <= this.maxRetries; attempt += 1) {
          try {
            await this.transport.send(peerId, message);
            this.metrics.sentBytes += bytes;
            this.metrics.sentMessages += 1;
            return bytes;
          } catch (error) {
            if (attempt === this.maxRetries) throw error;
            this.metrics.retries += 1;
            if (this.retryDelayMs > 0) {
              await new Promise((resolve) => setTimeout(resolve, this.retryDelayMs * (attempt + 1)));
            }
          }
        }
      } catch (error) {
        this.metrics.failedMessages += 1;
        throw error;
      } finally {
        state.queuedBytes -= bytes;
      }
    });
    state.tail = sendOperation.catch(() => {});
    this.queues.set(peerId, state);
    return sendOperation;
  }

  getStatus() {
    return {
      metrics: { ...this.metrics },
      queuedBytes: Object.fromEntries([...this.queues].map(([peerId, state]) =>
        [peerId, state.queuedBytes]))
    };
  }

  #estimateBytes(value) {
    if (value instanceof Uint8Array) return Math.ceil(value.byteLength * 4 / 3) + 32;
    if (typeof value === "string") return new TextEncoder().encode(value).byteLength + 2;
    if (value === null || typeof value !== "object") return String(value).length;
    if (Array.isArray(value)) {
      return 2 + value.reduce((size, item) => size + this.#estimateBytes(item) + 1, 0);
    }
    return 2 + Object.entries(value).reduce((size, [key, item]) =>
      size + this.#estimateBytes(key) + this.#estimateBytes(item) + 2, 0);
  }
}
