const BigSparseArray = require('big-sparse-array')
const { Transform } = require('streamx')
const quickbit = require('./compat').quickbit
const b4a = require('b4a')

const BITS_PER_PAGE = 32768
const BYTES_PER_PAGE = BITS_PER_PAGE / 8

class MarkPage {
  constructor() {
    this.bitfield = null
    this.loaded = new Promise((resolve) => {
      this.load = resolve
    })
  }

  setBitfield(bitfield) {
    this.bitfield = bitfield
    this.load()
  }

  get(index) {
    return quickbit.get(this.bitfield, index)
  }

  set(index, val) {
    quickbit.set(this.bitfield, index, val)
  }

  findFirst(val, position) {
    return quickbit.findFirst(this.bitfield, val, position)
  }

  findLast(val, position) {
    return quickbit.findLast(this.bitfield, val, position)
  }
}

module.exports = class MarkBitfield {
  static BITS_PER_PAGE = BITS_PER_PAGE
  static BYTES_PER_PAGE = BYTES_PER_PAGE

  constructor(storage) {
    this.storage = storage
    this._pages = new BigSparseArray()
  }

  async loadPage(pageIndex) {
    let p = this._pages.set(pageIndex, new MarkPage())
    const rx = this.storage.read()
    const pageBuf = rx.getMark(pageIndex)
    rx.tryFlush()
    const bitfield = (await pageBuf) ?? b4a.alloc(BYTES_PER_PAGE)
    await p.setBitfield(bitfield)
    return p
  }

  async get(index) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    const p = this._pages.get(i)
    if (!p) p = await this.loadPage(i)

    return p.get(j)
  }

  async set(index, val) {
    const j = index & (BITS_PER_PAGE - 1)
    const i = (index - j) / BITS_PER_PAGE

    let p = this._pages.get(i)

    if (!p && val) p = await this.loadPage(i)

    if (p) {
      await p.loaded
      p.set(j, val)
      const tx = this.storage.write()
      tx.putMark(i, p.bitfield)
      await tx.flush()
    }
  }

  async clear() {
    const tx = this.storage.write()
    tx.deleteMarkRange(0, -1)
    return tx.flush()
  }

  createMarkStream({ reverse = false }) {
    return this.storage.createMarkStream({ reverse }).pipe(
      new Transform({
        transform({ index, page }, cb) {
          let bitIndex = quickbit.findLast(page, true, BITS_PER_PAGE)
          while (bitIndex !== -1) {
            const blockIndex = index * BITS_PER_PAGE + bitIndex
            this.push(blockIndex)
            // Account for `bitIndex` being 0 causing infinite loop
            if (bitIndex === 0) break

            bitIndex = quickbit.findLast(page, true, bitIndex - 1)
          }

          cb(null)
        }
      })
    )
  }
}
