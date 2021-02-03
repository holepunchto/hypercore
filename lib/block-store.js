module.exports = class BlockStore {
  constructor (storage, tree) {
    this.storage = storage
    this.tree = tree
  }

  async get (i) {
    const [offset, size] = await this.tree.byteRange(2 * i)
    return this._read(offset, size)
  }

  async put (i, data) {
    const offset = i === this.tree.length ? this.tree.byteLength : await this.tree.byteOffset(2 * i)
    return this._write(offset, data)
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close((err) => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  putBatch (i, batch) {
    if (!batch.length) return Promise.resolve()
    return this.put(i, batch.length === 1 ? batch[0] : Buffer.concat(batch))
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
        else resolve()
      })
    })
  }
}
