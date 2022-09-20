const BigSparseArray = require('big-sparse-array')
const quickbit = require('quickbit-universal')

module.exports = class RemoteBitfield {
  constructor () {
    this._pageSize = 32768
    this._pages = new BigSparseArray()
  }

  get (index) {
    const j = index & (this._pageSize - 1)
    const i = (index - j) / this._pageSize

    const p = this._pages.get(i)

    return p ? quickbit.get(p, j) : false
  }

  set (index, val) {
    const j = index & (this._pageSize - 1)
    const i = (index - j) / this._pageSize

    const p = this._pages.get(i) || this._pages.set(i, new Uint32Array(this._pageSize / 32))

    quickbit.set(p, j, val)
  }

  setRange (start, length, val) {
    let j = start & (this._pageSize - 1)
    let i = (start - j) / this._pageSize

    while (length > 0) {
      const p = this._pages.get(i) || this._pages.set(i, new Uint32Array(this._pageSize / 32))

      const end = Math.min(j + length, this._pageSize)
      const range = end - j

      quickbit.fill(p, val, j, end)

      j = 0
      i++
      length -= range
    }
  }

  insert (start, bitfield) {
    if (start % 32 !== 0) return false

    let length = bitfield.byteLength * 8

    let j = start & (this._pageSize - 1)
    let i = (start - j) / this._pageSize

    while (length > 0) {
      const p = this._pages.get(i) || this._pages.set(i, new Uint32Array(this._pageSize / 32))

      const end = Math.min(j + length, this._pageSize)
      const range = end - j

      p.set(bitfield.subarray(0, range / 32), j / 32)

      bitfield = bitfield.subarray(range / 32)

      j = 0
      i++
      length -= range
    }

    return true
  }
}
