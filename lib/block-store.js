module.exports = class BlockStore {
  constructor (storage) {
    this.storage = storage
  }

  async get (reader, i) {
    return reader.getBlock(i)
  }

  async put (writer, i, data) {
    await writer.putBlock(i, data)
  }

  async putBatch (writer, i, blocks) {
    if (blocks.length === 0) return Promise.resolve()

    const p = []
    for (let j = 0; j < blocks.length; j++) {
      p.push(writer.putBlock(i + j, blocks[j]))
    }

    await Promise.all(p)
  }

  clear (writer, start, length = -1) {
    const end = length === -1 ? -1 : start + length
    return writer.deleteBlockRange(start, end)
  }
}
