module.exports = class BlockStore {
  constructor (storage) {
    this.storage = storage
  }

  async get (reader, id, i) {
    return reader.getBlock(id, i)
  }

  async put (writer, id, i, data) {
    await writer.putBlock(i, data)
  }

  async putBatch (writer, id, i, blocks) {
    if (blocks.length === 0) return Promise.resolve()

    const p = []
    for (let j = 0; j < blocks.length; j++) {
      p.push(writer.putBlock(id, i + j, blocks[j]))
    }

    await Promise.all(p)
  }

  clear (writer, id, start, length = -1) {
    const end = length === -1 ? -1 : start + length
    return writer.deleteBlockRange(id, start, end)
  }
}
