const NoiseStream = require('./noise-stream')

class AutoAlias {
  constructor (stream, discoveryKey, handlers, state) {
    this.discoveryKey = discoveryKey
    this.state = state
    this.stream = stream
    this.handlers = handlers
  }

  info (data) {
    this.stream.send(2, { discoveryKey: this.discoveryKey, data })
  }

  want (data) {
    this.stream.send(3, { discoveryKey: this.discoveryKey, data })
  }

  have (data) {
    this.stream.send(4, { discoveryKey: this.discoveryKey, data })
  }

  data (data) {
    this.stream.send(5, { discoveryKey: this.discoveryKey, data })
  }

  request (data) {
    this.stream.send(6, { discoveryKey: this.discoveryKey, data })
  }

  options (data) {
    this.stream.send(7, { discoveryKey: this.discoveryKey, data })
  }

  extension (data) {
    this.stream.send(8, { discoveryKey: this.discoveryKey, data })
  }
}

module.exports = class ProtocolStream extends NoiseStream {
  constructor (isInitiator) {
    super(isInitiator)

    this.aliases = new Map()
    this.pending = new Map()
    this.onerror = (err) => this.destroy(err)

    this.on('message', (type, m) => {
      if (type === 1) {
        this.emit('handshake', m.data)
        return
      }

      if (!m.discoveryKey) return

      const hex = m.discoveryKey.toString('hex')
      const a = this.aliases.get(hex)

      if (this.pending.size > 0 && this.pending.has(hex)) {
        this.pending.get(hex).push([type, m])
        return
      }

      if (!a) {
        this.emit('discovery-key', m.discoveryKey)
        this.pending.set(hex, [[type, m]])
        return
      }

      this._oncoremessage(a, type, m)
    })
  }

  handshake (data) {
    this.send(1, data)
  }

  alias (discoveryKey, handlers, state) {
    const hex = discoveryKey.toString('hex')
    const a = new AutoAlias(this, discoveryKey, handlers, state)

    this.aliases.set(hex, a)

    process.nextTick(() => {
      if (this.aliases.get(hex) !== a) return
      const buffered = this.pending.get(hex)
      if (!buffered) return
      this.pending.delete(hex)
      for (const [type, m] of buffered) {
        this._oncoremessage(a, type, m)
      }
    })

    return a
  }

  _oncoremessage (a, type, m) {
    switch (type) {
      case 2: return this._catch(a.handlers.oninfo(m.data, a))
      case 3: return this._catch(a.handlers.onwant(m.data, a))
      case 4: return this._catch(a.handlers.onhave(m.data, a))
      case 5: return this._catch(a.handlers.ondata(m.data, a))
      case 6: return this._catch(a.handlers.onrequest(m.data, a))
      case 7: return this._catch(a.handlers.onoptions(m.data, a))
      case 8: return this._catch(a.handlers.onextension(m.data, a))
    }
  }

  _catch (p) {
    if (p) p.catch(this.onerror)
  }
}
