module.exports = class Bitfield {
  constructor (storage, buf) {
    this.bitfield = (buf && buf.byteLength >= 4)
      ? new Uint32Array(buf.buffer, buf.byteOffset, Math.floor(buf.byteLength / 4))
      : new Uint32Array(1)
    this.unflushed = false
    this.storage = storage
  }

  get (index) {
    const j = index & 31
    const i = (index - j) / 32

    return i < this.bitfield.length && (this.bitfield[i] & (1 << j)) !== 0
  }

  set (index, val) {
    const j = index & 31
    const i = (index - j) / 32

    if (this.bitfield.length <= i) this._grow(i)

    const v = this.bitfield[i]

    if (val === ((v & (1 << j)) !== 0)) return

    const u = val
      ? v | (1 << j)
      : v ^ (1 << j)

    if (u === v) return

    this.unflushed = true
    this.bitfield[i] = u
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
    if (!this.unflushed) return

    const b = Buffer.from(this.bitfield.buffer, this.bitfield.byteOffset, this.bitfield.byteLength)

    return new Promise((resolve, reject) => {
      this.storage.write(0, b, (err) => {
        if (err) return reject(err)
        this.unflushed = false
        resolve()
      })
    })
  }

  _grow (i) {
    let len = this.bitfield.length
    while (i >= len) len *= 2

    const old = this.bitfield
    this.bitfield = new Uint32Array(len)
    this.bitfield.set(old)
    this.unflushed = true
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
