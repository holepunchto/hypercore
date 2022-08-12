module.exports = class Info {
  constructor (opts = {}) {
    this.length = opts.length || 0
    this.contiguousLength = opts.contiguousLength || 0
    this.byteLength = opts.byteLength || 0
    this.padding = opts.padding || 0
  }

  static async from (core, padding, snapshot) {
    return new Info({
      key: core.key,
      length: snapshot
        ? snapshot.length
        : core.tree.length,
      contiguousLength: core.header.contiguousLength,
      byteLength: snapshot
        ? snapshot.byteLength
        : (core.tree.byteLength - (core.tree.length * padding)),
      fork: core.tree.fork,
      padding
    })
  }
}
