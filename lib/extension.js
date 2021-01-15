const Message = require('abstract-extension')

module.exports = class Extension extends Message {
  broadcast (message) {
    const feed = this.local.handlers
    const data = this.encoding.encode(message)
    for (const peer of feed.peers) {
      peer.extension({ id: this.id, data })
    }
  }

  send (message, peer) {
    peer.extension({ id: this.id, data: this.encode(message) })
  }
}
