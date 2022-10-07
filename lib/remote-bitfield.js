const BigSparseArray = require('big-sparse-array')
const quickbit = require('./compat').quickbit

const BITS_PER_PAGE = 32768
const BYTES_PER_PAGE = BITS_PER_PAGE / 8
const WORDS_PER_PAGE = BYTES_PER_PAGE / 4
const BITS_PER_SEGMENT = 2097152
const BYTES_PER_SEGMENT = BITS_PER_SEGMENT / 8
const PAGES_PER_SEGMENT = BITS_PER_SEGMENT / BITS_PER_PAGE

class RemoteBitfieldPage {
  constructor (index, bitfield, segment) {
    this.index = index
    this.offset = index * BYTES_PER_PAGE - segment.offset
    this.bitfield = bitfield
    this.segment = segment

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

  findFirst (val, position) {
    return quickbit.findFirst(this.bitfield, val, position)
  }

  findLast (val, position) {
    return quickbit.findLast(this.bitfield, val, position)
  }

  insert (start, bitfield) {
    this.bitfield.set(bitfield, start / 32)
  }
}

class RemoteBitfieldSegment {
  constructor (index) {
    this.index = index
    this.offset = index * BYTES_PER_SEGMENT
    this.tree = quickbit.Index.from([])
    this.pages = new Array(PAGES_PER_SEGMENT)
  }

  get chunks () {
    return this.tree.chunks
  }

  add (page) {
    this.pages[page.index - this.index * PAGES_PER_SEGMENT] = page

    const chunk = { field: page.bitfield, offset: page.offset }

    this.chunks.push(chunk)

    for (let i = this.chunks.length - 2; i >= 0; i--) {
      const prev = this.chunks[i]
      if (prev.offset <= chunk.offset) break
      this.chunks[i] = chunk
      this.chunks[i + 1] = prev
    }
  }

  findFirst (val, position) {
    position = this.tree.skipFirst(!val, position)

    const j = position & (BITS_PER_PAGE - 1)
    const i = (position - j) / BITS_PER_PAGE

    if (i >= PAGES_PER_SEGMENT) return -1

    const p = this.pages[i]

    if (p) {
      const index = p.findFirst(val, j)

      if (index !== -1) {
        return i * BITS_PER_PAGE + index
      }
    }

    return -1
  }

  findLast (val, position) {
    position = this.tree.skipLast(!val, position)

    const j = position & (BITS_PER_PAGE - 1)
    const i = (position - j) / BITS_PER_PAGE

    if (i >= PAGES_PER_SEGMENT) return -1

    const p = this.pages[i]

    if (p) {
      const index = p.findLast(val, j)

      if (index !== -1) {
        return i * BITS_PER_PAGE + index
      }
    }

    return -1
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
      const k = Math.floor(i / PAGES_PER_SEGMENT)
      const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(k))

      p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(WORDS_PER_PAGE), s))
    }

    if (p) p.set(j, val)
  }

  setRange (start, length, val) {
    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p && val) {
        const k = Math.floor(i / PAGES_PER_SEGMENT)
        const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(k))

        p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(WORDS_PER_PAGE), s))
      }

      const end = Math.min(j + length, BITS_PER_PAGE)
      const range = end - j

      if (p) p.setRange(j, range, val)

      j = 0
      i++
      length -= range
    }
  }

  findFirst (val, position) {
    let j = position & (BITS_PER_SEGMENT - 1)
    let i = (position - j) / BITS_PER_SEGMENT

    while (i < this._segments.maxLength) {
      const s = this._segments.get(i)

      if (s) {
        const index = s.findFirst(val, j)

        if (index !== -1) {
          return i * BITS_PER_SEGMENT + index
        }
      }

      j = 0
      i++
    }

    return -1
  }

  firstSet (position) {
    return this.findFirst(true, position)
  }

  firstUnset (position) {
    return this.findFirst(false, position)
  }

  findLast (val, position) {
    let j = position & (BITS_PER_SEGMENT - 1)
    let i = (position - j) / BITS_PER_SEGMENT

    while (i >= 0) {
      const s = this._segments.get(i)

      if (s) {
        const index = s.findLast(val, j)

        if (index !== -1) {
          return i * BITS_PER_SEGMENT + index
        }
      }

      j = BITS_PER_SEGMENT - 1
      i--
    }

    return -1
  }

  lastSet (position) {
    return this.findLast(true, position)
  }

  lastUnset (position) {
    return this.findLast(false, position)
  }

  insert (start, bitfield) {
    if (start % 32 !== 0) return false

    let length = bitfield.byteLength * 8

    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p) {
        const k = Math.floor(i / PAGES_PER_SEGMENT)
        const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(k))

        p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(WORDS_PER_PAGE), s))
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
