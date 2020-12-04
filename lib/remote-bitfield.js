const BigSparseArray = require('big-sparse-array')

module.exports = class RemoteBitfield {
  constructor () {
    this.pages = new BigSparseArray()
  }

  get (index) {
    const r = index & 32767
    const i = (index - r) / 32768
    const p = this.pages.get(i)

    return p ? (p[r >>> 5] & (1 << (r & 31))) !== 0 : false
  }

  set (index, val) {
    const r = index & 32767
    const i = (index - r) / 32768
    const p = this.pages.get(i) || this.pages.set(i, new Uint32Array(1024))

    if (val) p[r >>> 5] |= (1 << (r & 31))
    else p[r >>> 5] &= ~(1 << (r & 31))
  }
}
