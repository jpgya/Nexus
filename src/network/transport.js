export class Transport {
  async send(_peerId, _message) {
    throw new Error("Transport.send must be implemented by the application");
  }

  async request(_peerId, _message, _options = {}) {
    throw new Error("Transport.request must be implemented by the application");
  }

  subscribe(_handler) {
    throw new Error("Transport.subscribe must be implemented by the application");
  }

  close() {
    throw new Error("Transport.close must be implemented by the application");
  }
}
