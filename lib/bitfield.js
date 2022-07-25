const BigSparseArray = require('big-sparse-array')
const b4a = require('b4a')
const bits = require('bits-to-bytes')

class FixedBitfield {
  constructor (index, bitfield) {
    this.dirty = false
    this.index = index
    this.bitfield = bitfield
  }

  get (index) {
    return bits.get(this.bitfield, index)
  }

  set (index, val) {
    return bits.set(this.bitfield, index, val)
  }

  setRange (start, length, val) {
    // Using fill instead of setRange is ~2 orders of magnitude faster, but does
    // have the downside of not being able to tell if any bits actually changed.
    bits.fill(this.bitfield, val, start, start + length)
    return true
  }
}

module.exports = class Bitfield {
  constructor (storage, buf) {
    this.pageSize = 32768
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
      : new Uint32Array(1024)

    for (let i = 0; i < all.length; i += 1024) {
      const bitfield = ensureSize(all.subarray(i, i + 1024), 1024)
      const page = new FixedBitfield(i / 1024, bitfield)
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
      p = this.pages.set(i, new FixedBitfield(i, new Uint32Array(1024)))
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
        p = this.pages.set(i, new FixedBitfield(i, new Uint32Array(1024)))
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

  clear () {
    return new Promise((resolve, reject) => {
      this.storage.del(0, Infinity, (err) => {
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
        this.storage.write(page.index * 4096, buf, done)
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

function ensureSize (uint32, size) {
  if (uint32.byteLength === size) return uint32
  const a = new Uint32Array(1024)
  a.set(uint32, 0)
  return a
}
