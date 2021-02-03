const BinaryStream = require('binary-message-stream')

class AutoAlias {
  constructor (stream, discoveryKey, handlers, state) {
    this.discoveryKey = discoveryKey
    this.state = state
    this.stream = stream
    this.handlers = handlers
  }

  info (data) {
    this.stream.send({ type: 'info', discoveryKey: this.discoveryKey, data })
  }

  want (data) {
    this.stream.send({ type: 'want', discoveryKey: this.discoveryKey, data })
  }

  have (data) {
    this.stream.send({ type: 'have', discoveryKey: this.discoveryKey, data })
  }

  data (data) {
    this.stream.send({ type: 'data', discoveryKey: this.discoveryKey, data })
  }

  request (data) {
    this.stream.send({ type: 'request', discoveryKey: this.discoveryKey, data })
  }

  options (data) {
    this.stream.send({ type: 'options', discoveryKey: this.discoveryKey, data })
  }

  extension (data) {
    this.stream.send({ type: 'extension', discoveryKey: this.discoveryKey, data })
  }
}

module.exports = class ProtocolStream extends BinaryStream {
  constructor () {
    super()

    this.aliases = new Map()
    this.pending = new Map()
    this.onerror = (err) => this.destroy(err)

    this.on('message', (m) => {
      if (m.type === 'handshake') {
        this.emit('handshake', m.data)
        return
      }

      if (!m.discoveryKey) return

      const hex = m.discoveryKey.toString('hex')
      const a = this.aliases.get(hex)

      if (!a) {
        if (this.pending.has(hex)) {
          this.pending.get(hex).push(m)
          return
        }

        this.emit('discovery-key', m.discoveryKey)
        this.pending.set(hex, [m])
        return
      }

      this._oncoremessage(a, m)
    })
  }

  handshake (data) {
    this.send({ type: 'handshake', data })
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
      for (const m of buffered) {
        this._oncoremessage(a, m)
      }
    })

    return a
  }

  _oncoremessage (a, m) {
    switch (m.type) {
      case 'info': return this._catch(a.handlers.oninfo(m.data, a))
      case 'want': return this._catch(a.handlers.onwant(m.data, a))
      case 'have': return this._catch(a.handlers.onhave(m.data, a))
      case 'data': return this._catch(a.handlers.ondata(m.data, a))
      case 'request': return this._catch(a.handlers.onrequest(m.data, a))
      case 'options': return this._catch(a.handlers.onoptions(m.data, a))
      case 'extension': return this._catch(a.handlers.onextension(m.data, a))
    }
  }

  _catch (p) {
    if (p) p.catch(this.onerror)
  }
}
