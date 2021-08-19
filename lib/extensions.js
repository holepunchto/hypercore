class Extension {
  constructor (extensions, name, handlers) {
    this.extensions = extensions
    this.name = name
    this.encoding = handlers.encoding
    this.destroyed = false
    // TODO: should avoid the bind here by calling directly on handlers instead?
    this.onmessage = (handlers.onmessage || noop).bind(handlers)
    this.onremotesupports = (handlers.onremotesupports || noop).bind(handlers)
  }

  send (message, peer) {
    if (this.destroyed) return
    const ext = peer.extensions.get(this.name)
    if (ext) ext.send(message)
  }

  broadcast (message) {
    if (this.extensions.replicator === null || this.destroyed) return
    for (const peer of this.extensions.replicator.peers) this.send(message, peer)
  }

  destroy () {
    if (this.destroyed) return
    this.destroyed = true
    this.extensions.all.delete(this.name)
    if (this.extensions.replicator === null) return
    for (const peer of this.extensions.replicator.peers) {
      const ext = peer.extensions.get(this.name)
      if (ext) ext.destroy()
    }
  }
}

module.exports = class Extensions {
  constructor () {
    this.replicator = null
    this.all = new Map()
  }

  [Symbol.iterator] () {
    return this.all[Symbol.iterator]()
  }

  attach (replicator) {
    if (replicator === this.replicator) return
    this.replicator = replicator

    for (const [name, ext] of this.all) {
      for (const peer of this.replicator.peers) {
        peer.registerExtension(name, ext)
      }
    }
  }

  register (name, handlers, ext = new Extension(this, name, handlers)) {
    if (this.all.has(name)) this.all.get(name).destroy()
    this.all.set(name, ext)

    if (this.replicator !== null) {
      for (const peer of this.replicator.peers) {
        peer.registerExtension(name, ext)
      }
    }

    return ext
  }

  update (peer) {
    for (const ext of this.all.values()) {
      peer.registerExtension(ext.name, ext)
    }
  }
}

function noop () {}
