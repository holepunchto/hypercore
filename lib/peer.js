const BinaryStream = require('binary-message-stream')

module.exports = class Peer {
  constructor (handlers, state) {
    this.stream = new BinaryStream()
    this.state = state
    this.onerror = (err) => handlers.onerror(err, this)

    this.stream.on('message', (m) => {
      switch (m.type) {
        case 'handshake': return this._catch(handlers.onhandshake(m.data, this))
        case 'info': return this._catch(handlers.oninfo(m.data, this))
        case 'want': return this._catch(handlers.onwant(m.data, this))
        case 'have': return this._catch(handlers.onhave(m.data, this))
        case 'data': return this._catch(handlers.ondata(m.data, this))
        case 'request': return this._catch(handlers.onrequest(m.data, this))
        case 'options': return this._catch(handlers.onoptions(m.data, this))
        case 'extension': return this._catch(handlers.onextension(m.data, this))
      }
    })
  }

  _catch (p) {
    if (p) p.catch(this.onerror)
  }

  handshake (data) {
    this.stream.send({ type: 'handshake', data })
  }

  info (data) {
    this.stream.send({ type: 'info', data })
  }

  want (data) {
    this.stream.send({ type: 'want', data })
  }

  have (data) {
    this.stream.send({ type: 'have', data })
  }

  data (data) {
    this.stream.send({ type: 'data', data })
  }

  request (data) {
    this.stream.send({ type: 'request', data })
  }

  options (data) {
    this.stream.send({ type: 'options', data })
  }

  extension (data) {
    this.stream.send({ type: 'extension', data })
  }
}
