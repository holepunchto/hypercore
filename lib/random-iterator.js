module.exports = class RandomIterator {
  constructor (values) {
    this._values = values
    this._next = { value: null, done: false }
    this._start = 0
    this._end = values.length
  }

  next () {
    if (this._start < this._values.length) {
      if (this._start === this._end) this._end = this._values.length

      const r = this._start + (Math.random() * (this._end - this._start)) | 0
      const tmp = this._values[r]

      this._values[r] = this._values[this._start]
      this._next.value = this._values[this._start++] = tmp
    } else {
      this._next.done = true
    }

    return this._next
  }

  reset () {
    this._start = 0
    this._end = this._values.length
    this._next.done = false
    return this
  }

  requeue () {
    if (this._start === this._end) {
      this._start--
    } else {
      const top = this._values[--this._end]

      this._values[this._end] = this._values[--this._start]
      this._values[this._start] = top
    }
  }

  [Symbol.iterator] () {
    return this
  }
}
