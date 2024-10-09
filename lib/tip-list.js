const { ASSERTION } = require('hypercore-errors')

module.exports = class TipList {
  constructor () {
    this.offset = 0
    this.removed = 0
    this.data = []
  }

  length () {
    return this.offset + this.data.length
  }

  end () {
    if (this.removed) return this.removed
    return this.offset + this.data.length
  }

  delete (start, end) {
    if (end < this.length() || (this.length() < start && this.length() !== 0)) {
      throw ASSERTION('Invalid delete on tip list')
    }

    if (this.end() === 0) this.offset = start
    if (this.removed < end) this.removed = end

    if (start < this.offset) {
      this.offset = start
      this.data = [] // clear everything
      return
    }

    while (this.length() > start) this.data.pop()
  }

  put (index, value) {
    if (this.end() === 0) {
      this.offset = index
      this.data.push(value)
      return
    }

    if (!this.removed && this.end() === index) {
      this.data.push(value)
      return
    }

    throw ASSERTION('Invalid put on tip list')
  }

  get (index) {
    index -= this.offset
    if (index >= this.data.length || index < 0) return null
    return this.data[index]
  }

  * [Symbol.iterator] () {
    for (let i = 0; i < this.data.length; i++) {
      yield [i + this.offset, this.data[i]]
    }
  }

  merge (tip) {
    if (this.end() < tip.offset || (tip.removed && tip.end() < this.end())) throw ASSERTION('Cannot merge tip list')
    while (this.end() !== tip.offset && tip.offset >= this.offset && tip.end() >= this.end()) this.data.pop()
    while (tip.removed && this.end() > tip.end()) this.data.pop()
    for (const data of tip.data) this.data.push(data)

    return this
  }
}
