const BigSparseArray = require('big-sparse-array')
const b4a = require('b4a')
const quickbit = require('./compat').quickbit

const BITS_PER_PAGE = 2097152
const BYTES_PER_PAGE = BITS_PER_PAGE / 8
const WORDS_PER_PAGE = BYTES_PER_PAGE / 4

class BitfieldPage {
  constructor (index, bitfield) {
    this.dirty = false
    this.index = index
    this.bitfield = bitfield
    this.tree = quickbit.Index.from(this.bitfield)
  }

  get (index) {
    return quickbit.get(this.bitfield, index)
  }

  set (index, val) {
    if (quickbit.set(this.bitfield, index, val)) {
      this.tree.update(index)
    }
  }

  setRange (start, length, val) {
    quickbit.fill(this.bitfield, val, start, start + length)

    let i = Math.floor(start / 32)
    const n = i + Math.ceil(length / 32)

    while (i < n) this.tree.update(i++ * 32)
  }

  findFirst (val, position) {
    return quickbit.findFirst(this.bitfield, val, this.tree.skipFirst(!val, position))
  }

  findLast (val, position) {
    return quickbit.findLast(this.bitfield, val, this.tree.skipLast(!val, position))
  }
}

module.exports = class Bitfield {
  constructor (storage, buf) {
    this.unflushed = []
    this.storage = storage
    this.resumed = !!(buf && buf.byteLength >= 4)

    this._pages = new BigSparseArray()

    const all = this.resumed
      ? new Uint32Array(buf.buffer, buf.byteOffset, Math.floor(buf.byteLength / 4))
      : new Uint32Array(WORDS_PER_PAGE)

    for (let i = 0; i < all.length; i += WORDS_PER_PAGE) {
      const bitfield = ensureSize(all.subarray(i, i + (WORDS_PER_PAGE)), WORDS_PER_PAGE)
      const page = new BitfieldPage(i / (WORDS_PER_PAGE), bitfield)
      this._pages.set(page.index, page)
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
      p = this._pages.set(i, new BitfieldPage(i, new Uint32Array(WORDS_PER_PAGE)))
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
        p = this._pages.set(i, new BitfieldPage(i, new Uint32Array(WORDS_PER_PAGE)))
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
    let j = position & (BITS_PER_PAGE - 1)
    let i = (position - j) / BITS_PER_PAGE

    while (i < this._pages.maxLength) {
      const p = this._pages.get(i)

      if (p) {
        const index = p.findFirst(val, j)

        if (index !== -1) {
          return i * BITS_PER_PAGE + index
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
    let j = position & (BITS_PER_PAGE - 1)
    let i = (position - j) / BITS_PER_PAGE

    while (i >= 0) {
      const p = this._pages.get(i)

      if (p) {
        const index = p.findLast(val, j)

        if (index !== -1) {
          return i * BITS_PER_PAGE + index
        }
      }

      j = BITS_PER_PAGE - 1
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

  * want (start, length) {
    const j = start & (BITS_PER_PAGE - 1)
    let i = (start - j) / BITS_PER_PAGE

    while (length > 0) {
      const p = this._pages.get(i)

      if (p) {
        // We always send at least 4 KiB worth of bitfield in a want, rounding
        // to the nearest 4 KiB.
        const end = ceilTo(clamp(length / 8, 4096, BYTES_PER_PAGE), 4096)

        yield {
          start: i * BITS_PER_PAGE,
          bitfield: p.bitfield.subarray(0, end / 4)
        }
      }

      i++
      length -= BITS_PER_PAGE
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

function ensureSize (buffer, size) {
  if (buffer.byteLength === size) return buffer
  const copy = new Uint32Array(size)
  copy.set(buffer, 0)
  return copy
}

function clamp (n, min, max) {
  return Math.min(Math.max(n, min), max)
}

function ceilTo (n, multiple = 1) {
  const remainder = n % multiple
  if (remainder === 0) return n
  return n + multiple - remainder
}
