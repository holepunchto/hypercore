module.exports = class Info {
  constructor(opts = {}) {
    this.key = opts.key
    this.discoveryKey = opts.discoveryKey
    this.length = opts.length || 0
    this.contiguousLength = opts.contiguousLength || 0
    this.byteLength = opts.byteLength || 0
    this.fork = opts.fork || 0
    this.padding = opts.padding || 0
    this.storage = opts.storage || null
  }

  static async from(session, opts = {}) {
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

  static async storage(session) {
    try {
      const storage = session.state && session.state.storage
      const db = storage && storage.db
      const ptr = storage.core && storage.core.dataPointer
      if (!db || ptr == null || typeof db.approximateSize !== 'function') return null

      // Keys are lexicographically ordered per core data section, so the byte
      // size of a section is the approximate size of its key range. The old
      // per-file split is mapped onto the rocksdb sections: the oplog covered
      // the core header, its dependencies and its hints.
      const { core } = require('hypercore-storage/lib/keys.js')
      const opts = { includeMemtables: true }
      const [oplog, tree, blocks, bitfield] = await Promise.all([
        db.approximateSize(core.head(ptr), core.block(ptr, 0), opts),
        db.approximateSize(core.tree(ptr, 0), core.bitfield(ptr, 0, 0), opts),
        db.approximateSize(core.block(ptr, 0), core.tree(ptr, 0), opts),
        db.approximateSize(core.bitfield(ptr, 0, 0), core.userData(ptr, ''), opts)
      ])
      return { oplog, tree, blocks, bitfield }
    } catch {
      return null
    }
  }

  static bytesUsed(file) {
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
