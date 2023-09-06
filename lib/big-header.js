const c = require('compact-encoding')
const { oplog } = require('./messages')

module.exports = class BigHeader {
  constructor (storage) {
    this.storage = storage
  }

  async load (external) {
    const buf = await new Promise((resolve, reject) => {
      this.storage.read(external.start, external.length, (err, buf) => {
        if (err) return reject(err)
        resolve(buf)
      })
    })

    const header = c.decode(oplog.header, buf)
    header.external = external
    return header
  }

  async flush (header) {
    const external = header.external || { start: 0, length: 0 }
    header.external = null

    const buf = c.encode(oplog.header, header)

    let start = 0
    if (buf.byteLength > external.start) {
      start = external.start + external.length
      const rem = start & 4095
      if (rem > 0) start += (4096 - rem)
    }

    header.external = { start, length: buf.byteLength }

    await new Promise((resolve, reject) => {
      this.storage.write(start, buf, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })

    return header
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close((err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }
}
