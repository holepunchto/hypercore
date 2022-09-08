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
    this.pageSize = 2097152
    this.pages = new BigSparseArray()
    this.unflushed = []
    this.storage = storage
    this.resumed = !!(buf && buf.byteLength >= 4)

    const all = this.resumed
      ? new Uint32Array(
          buf.buffer,
          buf.byteOffset,
          Math.floor(buf.byteLength / 4)
        )
      : new Uint32Array(this.pageSize / 32)

    for (let i = 0; i < all.length; i += this.pageSize / 32) {
      const bitfield = ensureSize(all.subarray(i, i + (this.pageSize / 32)), this.pageSize / 32)
      const page = new BitfieldPage(i / (this.pageSize / 32), bitfield)
      this.pages.set(page.index, page)
    }
  }

  get (index) {
    const j = index & (this.pageSize - 1)
    const i = (index - j) / this.pageSize

    const p = this.pages.get(i)

    return p ? p.get(j) : false
  }

  set (index, val) {
    const j = index & (this.pageSize - 1)
    const i = (index - j) / this.pageSize

    let p = this.pages.get(i)

    if (!p && val) {
      p = this.pages.set(i, new BitfieldPage(i, new Uint32Array(this.pageSize / 32)))
    }

    if (p && p.set(j, val) && !p.dirty) {
      p.dirty = true
      this.unflushed.push(p)
    }
  }

  setRange (start, length, val) {
    let j = start & (this.pageSize - 1)
    let i = (start - j) / this.pageSize

    while (length > 0) {
      let p = this.pages.get(i)

      if (!p && val) {
        p = this.pages.set(i, new BitfieldPage(i, new Uint32Array(this.pageSize / 32)))
      }

      const end = Math.min(j + length, this.pageSize)
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
    let j = position & (this.pageSize - 1)
    let i = (position - j) / this.pageSize

    while (i < this.pages.factor) {
      const p = this.pages.get(i)

      if (p) {
        const index = p.indexOf(val, j)

        if (index !== -1) {
          return i * this.pageSize + index
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
    let j = position & (this.pageSize - 1)
    let i = (position - j) / this.pageSize

    while (i >= 0) {
      const p = this.pages.get(i)

      if (p) {
        const index = p.lastIndexOf(val, j)

        if (index !== -1) {
          return i * this.pageSize + index
        }
      }

      j = this.pageSize - 1
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

  clear () {
    return new Promise((resolve, reject) => {
      this.storage.truncate(0, (err) => {
        if (err) return reject(err)
        this.pages = new BigSparseArray()
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
        this.storage.write(page.index * (this.pageSize / 8), buf, done)
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
