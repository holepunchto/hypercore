module.exports = class BlockStore {
  constructor (storage) {
    this.storage = storage
  }

  async get (rx, i) {
    return rx.getBlock(i)
  }

  put (tx, i, data) {
    tx.putBlock(i, data)
  }

  putBatch (tx, i, blocks) {
    if (blocks.length === 0) return Promise.resolve()

    for (let j = 0; j < blocks.length; j++) {
      tx.putBlock(i + j, blocks[j])
    }
  }

  clear (tx, start = 0, end = -1) {
    tx.deleteBlockRange(start, end)
  }
}
