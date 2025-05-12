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

  setRange (start, end, val) {
    quickbit.fill(this.bitfield, val, start, end)

    let i = Math.floor(start / 128)
    const n = i + Math.ceil((end - start) / 128)

    while (i <= n) this.tree.update(this.offset * 8 + i++ * 128)
  }

  findFirst (val, position) {
    return quickbit.findFirst(this.bitfield, val, position)
  }

  findLast (val, position) {
    return quickbit.findLast(this.bitfield, val, position)
  }

  insert (start, bitfield) {
    this.bitfield.set(bitfield, start / 32)
    this.segment.refresh()
  }

  clear (start, bitfield) {
    quickbit.clear(this.bitfield, { field: bitfield, offset: start })
  }
}

class RemoteBitfieldSegment {
  constructor (index) {
    this.index = index
    this.offset = index * BYTES_PER_SEGMENT
    this.tree = quickbit.Index.from([], BYTES_PER_SEGMENT)
    this.pages = new Array(PAGES_PER_SEGMENT)
    this.pagesLength = 0
  }

  get chunks () {
    return this.tree.chunks
  }

  refresh () {
    this.tree = quickbit.Index.from(this.tree.chunks, BYTES_PER_SEGMENT)
  }

  add (page) {
    const pageIndex = page.index - this.index * PAGES_PER_SEGMENT
    if (pageIndex >= this.pagesLength) this.pagesLength = pageIndex + 1

    this.pages[pageIndex] = page

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

    let j = position & (BITS_PER_PAGE - 1)
    let i = (position - j) / BITS_PER_PAGE

    if (i >= PAGES_PER_SEGMENT) return -1

    while (i < this.pagesLength) {
      const p = this.pages[i]

      let index = -1

      if (p) index = p.findFirst(val, j)
      else if (!val) index = j

      if (index !== -1) return i * BITS_PER_PAGE + index

      j = 0
      i++
    }

    return (val || this.pagesLength === PAGES_PER_SEGMENT) ? -1 : this.pagesLength * BITS_PER_PAGE
  }

  findLast (val, position) {
    position = this.tree.skipLast(!val, position)

    let j = position & (BITS_PER_PAGE - 1)
    let i = (position - j) / BITS_PER_PAGE

    if (i >= PAGES_PER_SEGMENT) return -1

    while (i >= 0) {
      const p = this.pages[i]

      let index = -1

      if (p) index = p.findLast(val, j)
      else if (!val) index = j

      if (index !== -1) return i * BITS_PER_PAGE + index

      j = BITS_PER_PAGE - 1
      i--
    }

    return -1
  }
}

module.exports = class RemoteBitfield {
  static BITS_PER_PAGE = BITS_PER_PAGE

  constructor () {
    this._pages = new BigSparseArray()
    this._segments = new BigSparseArray()
    this._maxSegments = 0
  }

  getBitfield (index) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    const p = this._pages.get(i)
    return p || null
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
      if (this._maxSegments <= k) this._maxSegments = k + 1

      p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(WORDS_PER_PAGE), s))
    }

    if (p) p.set(j, val)
  }

  setRange (start, end, val) {
    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (start < end) {
      let p = this._pages.get(i)

      if (!p && val) {
        const k = Math.floor(i / PAGES_PER_SEGMENT)
        const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(k))
        if (this._maxSegments <= k) this._maxSegments = k + 1

        p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(WORDS_PER_PAGE), s))
      }

      const offset = i * BITS_PER_PAGE
      const last = Math.min(end - offset, BITS_PER_PAGE)
      const range = last - j

      if (p) p.setRange(j, last, val)

      j = 0
      i++
      start += range
    }
  }

  findFirst (val, position) {
    let j = position & (BITS_PER_SEGMENT - 1)
    let i = (position - j) / BITS_PER_SEGMENT

    while (i < this._maxSegments) {
      const s = this._segments.get(i)

      let index = -1

      if (s) index = s.findFirst(val, j)
      else if (!val) index = j

      if (index !== -1) return i * BITS_PER_SEGMENT + index

      j = 0
      i++
    }

    // For the val === false case, we always return at least
    // the 'position', also if nothing was found
    return val
      ? -1
      : Math.max(position, this._maxSegments * BITS_PER_SEGMENT)
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

      let index = -1

      if (s) index = s.findLast(val, j)
      else if (!val) index = j

      if (index !== -1) return i * BITS_PER_SEGMENT + index

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
        if (this._maxSegments <= k) this._maxSegments = k + 1

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

  clear (start, bitfield) {
    if (start % 32 !== 0) return false

    let length = bitfield.byteLength * 8

    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p) {
        const k = Math.floor(i / PAGES_PER_SEGMENT)
        const s = this._segments.get(k) || this._segments.set(k, new RemoteBitfieldSegment(k))
        if (this._maxSegments <= k) this._maxSegments = k + 1

        p = this._pages.set(i, new RemoteBitfieldPage(i, new Uint32Array(WORDS_PER_PAGE), s))
      }

      const end = Math.min(j + length, BITS_PER_PAGE)
      const range = end - j

      p.clear(j, bitfield.subarray(0, range / 32))

      bitfield = bitfield.subarray(range / 32)

      j = 0
      i++
      length -= range
    }

    return true
  }
}
