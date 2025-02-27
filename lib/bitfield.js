const BigSparseArray = require('big-sparse-array')
const b4a = require('b4a')
const quickbit = require('./compat').quickbit

const BITS_PER_PAGE = 32768
const BYTES_PER_PAGE = BITS_PER_PAGE / 8
const WORDS_PER_PAGE = BYTES_PER_PAGE / 4
const BITS_PER_SEGMENT = 2097152
const BYTES_PER_SEGMENT = BITS_PER_SEGMENT / 8
const WORDS_PER_SEGMENT = BYTES_PER_SEGMENT / 4
const INITIAL_WORDS_PER_SEGMENT = 1024
const PAGES_PER_SEGMENT = BITS_PER_SEGMENT / BITS_PER_PAGE
const SEGMENT_GROWTH_FACTOR = 4

class BitfieldPage {
  constructor (index, segment) {
    this.index = index
    this.offset = index * BYTES_PER_PAGE - segment.offset
    this.bitfield = null
    this.segment = segment

    segment.add(this)
  }

  get tree () {
    return this.segment.tree
  }

  get (index, dirty) {
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

  count (start, length, val) {
    const end = start + length

    let i = start
    let c = 0

    while (length > 0) {
      const l = this.findFirst(val, i)
      if (l === -1 || l >= end) return c

      const h = this.findFirst(!val, l + 1)
      if (h === -1 || h >= end) return c + end - l

      c += h - l
      length -= h - i
      i = h
    }

    return c
  }
}

class BitfieldSegment {
  constructor (index, bitfield) {
    this.index = index
    this.offset = index * BYTES_PER_SEGMENT
    this.tree = quickbit.Index.from(bitfield, BYTES_PER_SEGMENT)
    this.pages = new Array(PAGES_PER_SEGMENT)
  }

  get bitfield () {
    return this.tree.field
  }

  add (page) {
    const i = page.index - this.index * PAGES_PER_SEGMENT
    this.pages[i] = page

    const start = i * WORDS_PER_PAGE
    const end = start + WORDS_PER_PAGE

    if (end >= this.bitfield.length) this.reallocate(end)

    page.bitfield = this.bitfield.subarray(start, end)
  }

  reallocate (length) {
    let target = this.bitfield.length
    while (target < length) target *= SEGMENT_GROWTH_FACTOR

    const bitfield = new Uint32Array(target)
    bitfield.set(this.bitfield)

    this.tree = quickbit.Index.from(bitfield, BYTES_PER_SEGMENT)

    for (let i = 0; i < this.pages.length; i++) {
      const page = this.pages[i]
      if (!page) continue

      const start = i * WORDS_PER_PAGE
      const end = start + WORDS_PER_PAGE

      page.bitfield = bitfield.subarray(start, end)
    }
  }

  findFirst (val, position) {
    position = this.tree.skipFirst(!val, position)

    let j = position & (BITS_PER_PAGE - 1)
    let i = (position - j) / BITS_PER_PAGE

    if (i >= PAGES_PER_SEGMENT) return -1

    while (i < this.pages.length) {
      const p = this.pages[i]

      let index = -1

      if (p) index = p.findFirst(val, j)
      else if (!val) index = j

      if (index !== -1) return i * BITS_PER_PAGE + index

      j = 0
      i++
    }

    return -1
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

module.exports = class Bitfield {
  static BITS_PER_PAGE = BITS_PER_PAGE
  static BYTES_PER_PAGE = BYTES_PER_PAGE

  constructor (buffer) {
    this.resumed = !!(buffer && buffer.byteLength >= 0)

    this._pages = new BigSparseArray()
    this._segments = new BigSparseArray()

    const view = this.resumed
      ? new Uint32Array(
        buffer.buffer,
        buffer.byteOffset,
        Math.floor(buffer.byteLength / 4)
      )
      : new Uint32Array(INITIAL_WORDS_PER_SEGMENT)

    for (let i = 0; i < view.length; i += WORDS_PER_SEGMENT) {
      let bitfield = view.subarray(i, i + (WORDS_PER_SEGMENT))
      let length = WORDS_PER_SEGMENT

      if (i === 0) {
        length = INITIAL_WORDS_PER_SEGMENT
        while (length < bitfield.length) length *= SEGMENT_GROWTH_FACTOR
      }

      if (bitfield.length !== length) {
        const copy = new Uint32Array(length)
        copy.set(bitfield, 0)
        bitfield = copy
      }

      const segment = new BitfieldSegment(i / (WORDS_PER_SEGMENT), bitfield)
      this._segments.set(segment.index, segment)

      for (let j = 0; j < bitfield.length; j += WORDS_PER_PAGE) {
        const page = new BitfieldPage((i + j) / WORDS_PER_PAGE, segment)
        this._pages.set(page.index, page)
      }
    }
  }

  static from (bitfield) {
    return new Bitfield(bitfield.toBuffer(bitfield._pages.maxLength * BITS_PER_PAGE))
  }

  toBuffer (length) {
    const pages = Math.ceil(length / BITS_PER_PAGE)
    const buffer = b4a.allocUnsafe(pages * BYTES_PER_PAGE)

    for (let i = 0; i < pages; i++) {
      const page = this._pages.get(i)
      const offset = i * BYTES_PER_PAGE

      if (page) {
        const buf = b4a.from(
          page.bitfield.buffer,
          page.bitfield.byteOffset,
          page.bitfield.byteLength
        )

        buffer.set(buf, offset)
      } else {
        buffer.fill(0, offset, offset + BYTES_PER_PAGE)
      }
    }

    return buffer
  }

  getBitfield (index) {
    const i = this.getPageIndex(index)

    const p = this._pages.get(i)
    return p || null
  }

  merge (bitfield, length) {
    let i = 0

    while (i < length) {
      const start = bitfield.firstSet(i)
      if (start === -1) break

      i = bitfield.firstUnset(start)

      if (i === -1 || i > length) i = length

      this.setRange(start, i, true)

      if (i >= length) break
    }
  }

  get (index) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    const p = this._pages.get(i)

    return p ? p.get(j) : false
  }

  getPageByteLength () {
    return BYTES_PER_PAGE
  }

  getPageIndex (index) {
    const j = index & (BITS_PER_PAGE - 1)
    return (index - j) / BITS_PER_PAGE
  }

  getPage (index, create) {
    const i = this.getPageIndex(index)

    let p = this._pages.get(i)

    if (p) return p

    if (!create) return null

    const k = Math.floor(i / PAGES_PER_SEGMENT)
    const s = this._segments.get(k) || this._segments.set(k, new BitfieldSegment(k, new Uint32Array(k === 0 ? INITIAL_WORDS_PER_SEGMENT : WORDS_PER_SEGMENT)))

    p = this._pages.set(i, new BitfieldPage(i, s))

    return p
  }

  set (index, val) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    let p = this._pages.get(i)

    if (!p && val) {
      const k = Math.floor(i / PAGES_PER_SEGMENT)
      const s = this._segments.get(k) || this._segments.set(k, new BitfieldSegment(k, new Uint32Array(k === 0 ? INITIAL_WORDS_PER_SEGMENT : WORDS_PER_SEGMENT)))

      p = this._pages.set(i, new BitfieldPage(i, s))
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
        const s = this._segments.get(k) || this._segments.set(k, new BitfieldSegment(k, new Uint32Array(k === 0 ? INITIAL_WORDS_PER_SEGMENT : WORDS_PER_SEGMENT)))

        p = this._pages.set(i, new BitfieldPage(i, s))
      }

      const offset = p.index * BITS_PER_PAGE
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

    while (i < this._segments.maxLength) {
      const s = this._segments.get(i)

      let index = -1

      if (s) index = s.findFirst(val, j)
      else if (!val) index = j

      if (index !== -1) return i * BITS_PER_SEGMENT + index

      j = 0
      i++
    }

    return val ? -1 : this._segments.maxLength * BITS_PER_SEGMENT
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

  count (start, length, val) {
    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE
    let c = 0

    while (length > 0) {
      const p = this._pages.get(i)

      const end = Math.min(j + length, BITS_PER_PAGE)
      const range = end - j

      if (p) c += p.count(j, range, val)
      else if (!val) c += range

      j = 0
      i++
      length -= range
    }

    return c
  }

  countSet (start, length) {
    return this.count(start, length, true)
  }

  countUnset (start, length) {
    return this.count(start, length, false)
  }

  * want (start, length) {
    const j = start & (BITS_PER_SEGMENT - 1)
    let i = (start - j) / BITS_PER_SEGMENT

    while (length > 0) {
      const s = this._segments.get(i)

      if (s) {
        // We always send at least 4 KiB worth of bitfield in a want, rounding
        // to the nearest 4 KiB.
        const end = ceilTo(clamp(length / 8, 4096, BYTES_PER_SEGMENT), 4096)

        yield {
          start: i * BITS_PER_SEGMENT,
          bitfield: s.bitfield.subarray(0, end / 4)
        }
      }

      i++
      length -= BITS_PER_SEGMENT
    }
  }

  clear (tx) {
    return tx.deleteBitfieldPageRange(0, -1)
  }

  onupdate (ranges) {
    for (const { start, end, value } of ranges) {
      this.setRange(start, end, value)
    }
  }

  static async open (storage, length) {
    if (length === 0) return new Bitfield(storage, null)

    const pages = Math.ceil(length / BITS_PER_PAGE)
    const buffer = b4a.alloc(pages * BYTES_PER_PAGE)
    const stream = storage.createBitfieldStream()

    for await (const { index, page } of stream) {
      buffer.set(page, index * BYTES_PER_PAGE)
    }

    return new Bitfield(buffer)
  }
}

function clamp (n, min, max) {
  return Math.min(Math.max(n, min), max)
}

function ceilTo (n, multiple = 1) {
  const remainder = n % multiple
  if (remainder === 0) return n
  return n + multiple - remainder
}
