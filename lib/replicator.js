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

    const { index, value } = proof.block || { index: this.log.length - 1, value: null }

    for (let i = 0; i < this.requests.length; i++) {
      const req = this.requests[i]

      if (req.seeker) {
        if (req.seeker.start <= index && (req.seeker.end === 0 || index < req.seeker.end)) {
          const res = await req.seeker.update()
          if (res) {
            this.requests.splice(i, 1)
            req.resolve(res)
            i--
          } else {
            this._spam()
          }
        }
        continue
      }

      if (req.index === index && value) {
        this.requests.splice(i, 1)
        i--
        req.resolve(proof.block.value)
      }
    }
  }

  async _spam () {
    for (const { index, seeker } of this.requests) {
      const min = seeker ? seeker.start : index

      for (const s of this.streams) {
        if (s.length <= min) continue

        const upgrade = s.length > this.log.tree.length ? { start: this.log.tree.length, length: s.length - this.log.tree.length } : null

        if (seeker) {
          const end = seeker.end ? Math.min(seeker.end, s.length) : s.length
          const nodes = await seeker.nodes()
          const index = Math.floor(Math.random() * (end - seeker.start) + seeker.start)
          const block = { index, nodes, value: false, bytes: seeker.bytes }
          s.send({ type: 'request', data: { block, upgrade } })
          continue
        }

        const nodes = await this.log.tree.nodes(2 * index)
        const block = { index, nodes, value: true, bytes: 0 }
        s.send({ type: 'request', data: { block, upgrade } })
      }
    }
  }

  seek (seeker) {
    const p = new Promise((resolve, reject) => {
      this.requests.push({
        seeker,
        resolve,
        reject
      })
    })

    this._spam()

    return p
  }

  async get (index) {
    const p = new Promise((resolve, reject) => {
      this.requests.push({
        index,
        resolve,
        reject
      })
    })

    this._spam()

    return p
  }
}
