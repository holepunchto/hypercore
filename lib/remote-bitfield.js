const BigSparseArray = require('big-sparse-array')
const quickbit = require('quickbit-universal')

const BITS_PER_PAGE = 32768
const BYTES_PER_PAGE = BITS_PER_PAGE / 8
const BITS_PER_SEGMENT = 2097152
const BYTES_PER_SEGMENT = BITS_PER_SEGMENT / 8
const PAGES_PER_SEGMENT = BITS_PER_SEGMENT / BITS_PER_PAGE

class RemoteBitfieldPage {
  constructor (index, bitfield, segment) {
    this.index = index
    this.bitfield = bitfield
    this.segment = segment
    this.offset = index * BYTES_PER_PAGE - segment.index * BYTES_PER_SEGMENT

    segment.add(this)
  }

  get tree () {
    return this.segment.tree
  }

  get (index) {
    return quickbit.get(this.bitfield, index)
  }

  set (index, val) {
    if (quickbit.set(this.bitfield, index, val)) {
      this.tree.update(this.offset * 8 + index)
    }
  }

  setRange (start, length, val) {
    quickbit.fill(this.bitfield, val, start, start + length)

    let i = Math.floor(start / 32)
    const n = i + Math.ceil(length / 32)

    while (i < n) this.tree.update(this.offset * 8 + i++ * 32)
  }

  insert (start, bitfield) {
    this.bitfield.set(bitfield, start / 32)
  }
}

class RemoteBitfieldSegment {
  constructor (index) {
    this.index = index
    this.tree = quickbit.Index.from([])
  }

  get chunks () {
    return this.tree.chunks
  }

  add (page) {
    let j = -1
    let i = this.chunks.length

    while (j + 1 < i) {
      const m = j + ((i - j) >>> 1)

      if (page.index < this.chunks[m].index) {
        i = m
      } else {
        j = m
      }
    }

    this.chunks.splice(i, 0, { field: page.bitfield, offset: page.offset })
  }
}

module.exports = class RemoteBitfield {
  constructor () {
    this._pages = new BigSparseArray()
    this._segments = new BigSparseArray()
  }

  get (index) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    const p = this._pages.get(i)

    return p ? p.get(j) : false
  }

  set (index, val) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    let p = this._pages.get(i)

    if (!p && val) {
      const k = i / PAGES_PER_SEGMENT | 0
      const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(i))

      p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(BITS_PER_PAGE / 32), s))
    }

    if (p) p.set(j, val)
  }

  setRange (start, length, val) {
    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p && val) {
        const k = i / PAGES_PER_SEGMENT | 0
        const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(i))

        p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(BITS_PER_PAGE / 32), s))
      }

      const end = Math.min(j + length, BITS_PER_PAGE)
      const range = end - j

      if (p) p.setRange(j, range, val)

      j = 0
      i++
      length -= range
    }
  }

  insert (start, bitfield) {
    if (start % 32 !== 0) return false

    let length = bitfield.byteLength * 8

    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p) {
        const k = i / PAGES_PER_SEGMENT | 0
        const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(i))

        p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(BITS_PER_PAGE / 32), s))
      }

      const end = Math.min(j + length, BITS_PER_PAGE)
      const range = end - j

      p.insert(j, bitfield.subarray(0, range / 32))

      bitfield = bitfield.subarray(range / 32)

      j = 0
      i++
      length -= range
    }

    return true
  }
}
