module.exports = class BlockStore {
  constructor (storage) {
    this.storage = storage
  }

  async get (reader, i) {
    return reader.getBlock(i)
  }

  put (writer, i, data) {
    writer.putBlock(i, data)
  }

  putBatch (writer, i, blocks) {
    if (blocks.length === 0) return Promise.resolve()

    for (let j = 0; j < blocks.length; j++) {
      writer.putBlock(i + j, blocks[j])
    }
  }

  clear (writer, start = 0, end = -1) {
    writer.deleteBlockRange(start, end)
  }
}
