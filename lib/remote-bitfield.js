const BigSparseArray = require('big-sparse-array')
const bits = require('bits-to-bytes')

module.exports = class RemoteBitfield {
  constructor () {
    this.pageSize = 32768
    this.pages = new BigSparseArray()
  }

  get (index) {
    const j = index & (this.pageSize - 1)
    const i = (index - j) / this.pageSize

    const p = this.pages.get(i)

    return p ? bits.get(p, j) : false
  }

  set (index, val) {
    const j = index & (this.pageSize - 1)
    const i = (index - j) / this.pageSize

    const p = this.pages.get(i) || this.pages.set(i, new Uint32Array(1024))

    bits.set(p, j, val)
  }

  setRange (start, length, val) {
    let j = start & (this.pageSize - 1)
    let i = (start - j) / this.pageSize

    while (length > 0) {
      const p = this.pages.get(i) || this.pages.set(i, new Uint32Array(1024))

      const end = Math.min(j + length, this.pageSize)
      const range = end - j

      bits.fill(p, val, j, end)

      j = 0
      i++
      length -= range
    }
  }
}
