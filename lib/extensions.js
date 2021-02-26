class Extension {
  constructor (extensions, name, handlers) {
    this.core = extensions.core
    this.extensions = extensions
    this.name = name
    this.encoding = handlers.encoding
    this.onmessage = handlers.onmessage || noop
    this.onremotesupports = handlers.onremotesupports || noop
  }

  send (message, peer) {
    const ext = peer.extensions.get(this.name)
    if (ext) ext.send(message)
  }

  broadcast (message) {
    if (this.core.replicator === null) return
    for (const peer of this.core.replicator.peers) this.send(message, peer)
  }

  destroy () {
    this.extensions.all.delete(this.name)
    for (const peer of this.core.replicator.peers) {
      const ext = peer.extensions.get(this.name)
      if (ext) ext.destroy()
    }
  }
}

module.exports = class Extensions {
  constructor (core) {
    this.core = core
    this.all = new Map()
  }

  register (name, handlers) {
    if (this.all.has(name)) return this.all.get(name)

    const ext = new Extension(this, name, handlers)
    this.all.set(name, ext)

    if (this.core.replicator !== null) {
      for (const peer of this.core.replicator.peers) {
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
