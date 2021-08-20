// TODO: needs massive improvements obvs

const BigSparseArray = require('big-sparse-array')

class FixedBitfield {
  constructor (index, bitfield) {
    this.dirty = false
    this.index = index
    this.bitfield = bitfield
  }

  get (index) {
    const j = index & 31
    const i = (index - j) / 32

    return i < this.bitfield.length && (this.bitfield[i] & (1 << j)) !== 0
  }

  set (index, val) {
    const j = index & 31
    const i = (index - j) / 32

    if (this.bitfield.length <= i) {
      console.log('???', index, this.bitfield.length, this.bitfield)
      this._grow(i)
    }

    const v = this.bitfield[i]

    if (val === ((v & (1 << j)) !== 0)) return false

    const u = val
      ? v | (1 << j)
      : v ^ (1 << j)

    if (u === v) return false

    this.bitfield[i] = u
    return true
  }
}

module.exports = class Bitfield {
  constructor (storage, buf) {
    this.pageSize = 32768
    this.pages = new BigSparseArray()
    this.unflushed = []
    this.storage = storage

    const all = (buf && buf.byteLength >= 4)
      ? new Uint32Array(buf.buffer, buf.byteOffset, Math.floor(buf.byteLength / 4))
      : new Uint32Array(1024)

    for (let i = 0; i < all.length; i += 32768) {
      const bitfield = ensureSize(all.subarray(i, i + 32768), 32768)
      this.pages.set(i, new FixedBitfield(i / 32768, bitfield))
    }
  }

  get (index) {
    const j = index & 32767
    const i = (index - j) / 32768
    const p = this.pages.get(i)

    return p ? p.get(j) : false
  }

  set (index, val) {
    const j = index & 32767
    const i = (index - j) / 32768

    let p = this.pages.get(i)

    if (!p) {
      if (!val) return
      p = this.pages.set(i, new FixedBitfield(i, new Uint32Array(1024)))
    }

    if (!p.set(j, val) || p.dirty) return

    p.dirty = true
    this.unflushed.push(p)
  }

  setRange (start, length, val) {
    for (let i = 0; i < length; i++) {
      this.set(start + i, val)
    }
  }

  // Should prob be removed, when/if we re-add compression
  page (i) {
    const p = this.pages.get(i)
    return p ? p.bitfield : new Uint32Array(1024)
  }

  clear () {
    return new Promise((resolve, reject) => {
      this.storage.del(0, Infinity, (err) => {
        if (err) reject(err)
        else resolve()
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
        const b = Buffer.from(page.bitfield.buffer, page.bitfield.byteOffset, page.bitfield.byteLength)
        page.dirty = false
        this.storage.write(page.index * 4096, b, done)
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
  if (uint32.length === size) return uint32
  const a = new Uint32Array(1024)
  a.set(uint32, 0)
  return a
}
