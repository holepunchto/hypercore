const BinaryStream = require('binary-message-stream')

module.exports = class Replicator {
  constructor (log) {
    this.log = log
    this.streams = new Set()
    this.length = 0
    this.requests = []
  }

  update () {
    if (this.log.tree.length === this.length) return
    this.length = this.log.tree.length
    for (const s of this.streams) {
      s.send({ type: 'info', data: { length: this.length } })
    }
  }

  createStream () {
    const s = new BinaryStream()
    const destroy = s.destroy.bind(s)

    s.length = 0
    this.streams.add(s)

    s.on('close', () => {
      this.streams.delete(s)
    })

    s.on('message', (message) => {
      switch (message.type) {
        case 'request': return this._onrequest(message.data, s).catch(destroy)
        case 'proof': return this._onproof(message.data, s).catch(destroy)
        case 'info': return this._oninfo(message.data, s).catch(destroy)
      }
    })

    this.log.opening.then(() => s.send({ type: 'info', data: { length: this.log.tree.length } }))

    return s
  }

  async _oninfo (info, s) {
    s.length = info.length
    this._spam()
  }

  async _onrequest (req, s) {
    const proof = await this.log.proof(req)
    s.send({ type: 'proof', data: proof })
  }

  async _onproof (proof, s) {
    await this.log.verify(proof)

    if (!proof.block || !proof.block.value) return
    const { index, value } = proof.block

    for (let i = 0; i < this.requests.length; i++) {
      const req = this.requests[i]

      if (req.index === index) {
        this.requests.splice(i, 1)
        req.resolve(proof.block.value)
      }
    }
  }

  _spam () {
    for (const { index, nodes } of this.requests) {
      for (const s of this.streams) {
        if (s.length <= index) continue
        const upgrade = s.length > this.log.tree.length ? { start: this.log.tree.length, length: s.length - this.log.tree.length } : null
        const block = { index, nodes, value: true }
        s.send({ type: 'request', data: { block, upgrade } })
      }
    }
  }

  async get (index) {
    const nodes = await this.log.tree.nodes(2 * index)

    const p = new Promise((resolve, reject) => {
      this.requests.push({
        index,
        nodes,
        resolve,
        reject
      })
    })

    this._spam()

    return p
  }
}
