const BigSparseArray = require('big-sparse-array')
const b4a = require('b4a')
const quickbit = require('quickbit-universal')

class BitfieldPage {
  constructor (index, bitfield) {
    this.dirty = false
    this.index = index
    this.bitfield = bitfield
    this.tree = new quickbit.Index(this.bitfield)
  }

  get (index) {
    return quickbit.get(this.bitfield, index)
  }

  set (index, val) {
    const changed = quickbit.set(this.bitfield, index, val)

    this.tree.update(index)

    return changed
  }

  setRange (start, length, val) {
    quickbit.fill(this.bitfield, val, start, start + length)

    let i = Math.floor(start / 32)
    const n = i + Math.ceil(length / 32)

    while (i < n) this.tree.update(i++ * 32)

    return true
  }

  indexOf (val, position) {
    return quickbit.indexOf(this.bitfield, val, position, this.tree)
  }

  lastIndexOf (val, position) {
    return quickbit.lastIndexOf(this.bitfield, val, position, this.tree)
  }
}

module.exports = class Bitfield {
  constructor (storage, buf) {
    this.unflushed = []
    this.storage = storage
    this.resumed = !!(buf && buf.byteLength >= 4)

    this._pageSize = 2097152
    this._pages = new BigSparseArray()

    const all = this.resumed
      ? new Uint32Array(buf.buffer, buf.byteOffset, Math.floor(buf.byteLength / 4))
      : new Uint32Array(this._pageSize / 32)

    for (let i = 0; i < all.length; i += this._pageSize / 32) {
      const bitfield = ensureSize(all.subarray(i, i + (this._pageSize / 32)), this._pageSize / 32)
      const page = new BitfieldPage(i / (this._pageSize / 32), bitfield)
      this._pages.set(page.index, page)
    }
  }

  get (index) {
    const j = index & (this._pageSize - 1)
    const i = (index - j) / this._pageSize

    const p = this._pages.get(i)

    return p ? p.get(j) : false
  }

  set (index, val) {
    const j = index & (this._pageSize - 1)
    const i = (index - j) / this._pageSize

    let p = this._pages.get(i)

    if (!p && val) {
      p = this._pages.set(i, new BitfieldPage(i, new Uint32Array(this._pageSize / 32)))
    }

    if (p && p.set(j, val) && !p.dirty) {
      p.dirty = true
      this.unflushed.push(p)
    }
  }

  setRange (start, length, val) {
    let j = start & (this._pageSize - 1)
    let i = (start - j) / this._pageSize

    while (length > 0) {
      let p = this._pages.get(i)

      if (!p && val) {
        p = this._pages.set(i, new BitfieldPage(i, new Uint32Array(this._pageSize / 32)))
      }

      const end = Math.min(j + length, this._pageSize)
      const range = end - j

      if (p && p.setRange(j, range, val) && !p.dirty) {
        p.dirty = true
        this.unflushed.push(p)
      }

      j = 0
      i++
      length -= range
    }
  }

  indexOf (val, position) {
    let j = position & (this._pageSize - 1)
    let i = (position - j) / this._pageSize

    while (i < this._pages.factor) {
      const p = this._pages.get(i)

      if (p) {
        const index = p.indexOf(val, j)

        if (index !== -1) {
          return i * this._pageSize + index
        }
      }

      j = 0
      i++
    }

    return -1
  }

  firstSet (position) {
    return this.indexOf(true, position)
  }

  firstUnset (position) {
    return this.indexOf(false, position)
  }

  lastIndexOf (val, position) {
    let j = position & (this._pageSize - 1)
    let i = (position - j) / this._pageSize

    while (i >= 0) {
      const p = this._pages.get(i)

      if (p) {
        const index = p.lastIndexOf(val, j)

        if (index !== -1) {
          return i * this._pageSize + index
        }
      }

      j = this._pageSize - 1
      i--
    }

    return -1
  }

  lastSet (position) {
    return this.lastIndexOf(true, position)
  }

  lastUnset (position) {
    return this.lastIndexOf(false, position)
  }

  * want (start, length) {
    const j = start & (this._pageSize - 1)
    let i = (start - j) / this._pageSize

    while (length > 0) {
      const p = this._pages.get(i)

      if (p) {
        // We always send at least 4 KiB worth of bitfield in a want, rounding
        // to the nearest 4 KiB.
        const end = ceilTo(clamp(length / 8, 4096, this._pageSize / 8), 4096)

        yield {
          start: i * this._pageSize,
          bitfield: p.bitfield.subarray(0, end / 4)
        }
      }

      i++
      length -= this._pageSize
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
        this.storage.write(page.index * (this._pageSize / 8), buf, done)
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

  static open (storage) {
    return new Promise((resolve, reject) => {
      storage.stat((err, st) => {
        if (err) return resolve(new Bitfield(storage, null))
        const size = st.size - (st.size & 3)
        if (!size) return resolve(new Bitfield(storage, null))
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
