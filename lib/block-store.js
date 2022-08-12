const b4a = require('b4a')

module.exports = class BlockStore {
  constructor (storage, tree) {
    this.storage = storage
    this.tree = tree
  }

  async get (i) {
    const [offset, size] = await this.tree.byteRange(2 * i)
    return this._read(offset, size)
  }

  async put (i, data, offset) {
    return this._write(offset, data)
  }

  putBatch (i, batch, offset) {
    if (batch.length === 0) return Promise.resolve()
    return this.put(i, batch.length === 1 ? batch[0] : b4a.concat(batch), offset)
  }

  clear (offset = 0, length = Infinity) {
    return new Promise((resolve, reject) => {
      this.storage.del(offset, length, (err) => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close((err) => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  _read (offset, size) {
    return new Promise((resolve, reject) => {
      this.storage.read(offset, size, (err, data) => {
        if (err) reject(err)
        else resolve(data)
      })
    })
  }

  _write (offset, data) {
    return new Promise((resolve, reject) => {
      this.storage.write(offset, data, (err) => {
        if (err) reject(err)
        else resolve(offset + data.byteLength)
      })
    })
  }
}
