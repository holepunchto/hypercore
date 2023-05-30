module.exports = class Info {
  constructor (opts = {}) {
    this.key = opts.key
    this.discoveryKey = opts.discoveryKey
    this.length = opts.length || 0
    this.contiguousLength = opts.contiguousLength || 0
    this.byteLength = opts.byteLength || 0
    this.fork = opts.fork || 0
    this.padding = opts.padding || 0
    this.storage = opts.storage || null
  }

  static async from (session, opts = {}) {
    return new Info({
      key: session.key,
      discoveryKey: session.discoveryKey,
      length: session.length,
      contiguousLength: session.contiguousLength,
      byteLength: session.byteLength,
      fork: session.fork,
      padding: session.padding,
      storage: opts.storage ? await this.storage(session) : null
    })
  }

  static async storage (session) {
    const { oplog, tree, blocks, bitfield } = session.core
    try {
      return {
        oplog: await Info.bytesUsed(oplog.storage),
        tree: await Info.bytesUsed(tree.storage),
        blocks: await Info.bytesUsed(blocks.storage),
        bitfield: await Info.bytesUsed(bitfield.storage)
      }
    } catch {
      return null
    }
  }

  static bytesUsed (file) {
    return new Promise((resolve, reject) => {
      file.stat((err, st) => {
        if (err) {
          resolve(0) // prob just file not found (TODO, improve)
        } else if (typeof st.blocks !== 'number') {
          reject(new Error('cannot determine bytes used'))
        } else {
          resolve(st.blocks * 512)
        }
      })
    })
  }
}
