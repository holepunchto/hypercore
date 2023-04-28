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
    this.dirty = false
    this.index = index
    this.offset = index * BYTES_PER_PAGE - segment.offset
    this.bitfield = null
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
    this.tree = quickbit.Index.from(bitfield)
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

    this.tree = quickbit.Index.from(bitfield)

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

module.exports = class Bitfield {
  constructor (storage, buffer) {
    this.unflushed = []
    this.storage = storage
    this.resumed = !!(buffer && buffer.byteLength >= 4)

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
      const s = this._segments.get(k) || this._segments.set(k, new BitfieldSegment(k, new Uint32Array(k === 0 ? INITIAL_WORDS_PER_SEGMENT : WORDS_PER_SEGMENT)))

      p = this._pages.set(i, new BitfieldPage(i, s))
    }

    if (p) {
      p.set(j, val)

      if (!p.dirty) {
        p.dirty = true
        this.unflushed.push(p)
      }
    }
  }

  setRange (start, length, val) {
    let j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p && val) {
        const k = Math.floor(i / PAGES_PER_SEGMENT)
        const s = this._segments.get(k) || this._segments.set(k, new BitfieldSegment(k, new Uint32Array(k === 0 ? INITIAL_WORDS_PER_SEGMENT : WORDS_PER_SEGMENT)))

        p = this._pages.set(i, new BitfieldPage(i, s))
      }

      const end = Math.min(j + length, BITS_PER_PAGE)
      const range = end - j

      if (p) {
        p.setRange(j, range, val)

        if (!p.dirty) {
          p.dirty = true
          this.unflushed.push(p)
        }
      }

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

  clear () {
    return new Promise((resolve, reject) => {
      this.storage.truncate(0, (err) => {
        if (err) return reject(err)
        this._pages = new BigSparseArray()
        this.unflushed = []
        resolve()
      })
    })
  }

  close () {
    return new Promise((resolve, reject) => {
      this.storage.close((err) => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  flush () {
    return new Promise((resolve, reject) => {
      if (!this.unflushed.length) return resolve()

      const self = this
      let missing = this.unflushed.length
      let error = null

      for (const page of this.unflushed) {
        const buf = b4a.from(
          page.bitfield.buffer,
          page.bitfield.byteOffset,
          page.bitfield.byteLength
        )

        page.dirty = false
        this.storage.write(page.index * BYTES_PER_PAGE, buf, done)
      }

      function done (err) {
        if (err) error = err
        if (--missing) return
        if (error) return reject(error)
        self.unflushed = []
        resolve()
      }
    })
  }

  static open (storage, tree = null) {
    return new Promise((resolve, reject) => {
      storage.stat((err, st) => {
        if (err) return resolve(new Bitfield(storage, null))
        let size = st.size - (st.size & 3)
        if (!size) return resolve(new Bitfield(storage, null))
        if (tree) size = Math.min(size, ceilTo(tree.length / 8, 4096))
        storage.read(0, size, (err, data) => {
          if (err) return reject(err)
          resolve(new Bitfield(storage, data))
        })
      })
    })
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
