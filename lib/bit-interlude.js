const assert = require('nanoassert')
const b4a = require('b4a')

module.exports = class BitInterlude {
  constructor (bitfield) {
    this.drop = false
    this.bitfield = bitfield
    this.ranges = []
  }

  contiguousLength (from) {
    if (this.drop && this.ranges.length > 0 && this.ranges[0].start < from) {
      return this.ranges[0].start
    }

    // TODO: be smarter
    while (this.get(from) === true) from++
    return from
  }

  get (index) {
    let start = 0
    let end = this.ranges.length

    while (start < end) {
      const mid = (start + end) >> 1
      const r = this.ranges[mid]

      if (index < r.start) {
        end = mid
        continue
      }

      if (index >= r.end) {
        if (mid === start) break
        start = mid
        continue
      }

      return this.drop === false
    }

    return this.bitfield.get(index)
  }

  setRange (start, end, value) {
    assert(this.drop !== value || this.ranges.length === 0)
    assert(value === true || this.ranges.length === 0)

    this.drop = value === false

    let r = null

    for (let i = 0; i < this.ranges.length; i++) {
      r = this.ranges[i]

      // if already inside, stop
      if (r.start <= start && end <= r.end) return

      // we wanna overun the interval
      if (start > r.end) {
        continue
      }

      // we overran but this interval is ending after us, move it back
      if (end >= r.start && end <= r.end) {
        r.start = start
        return
      }

      // we overran but our start is contained in this interval, move start back
      if (start >= r.start && start <= r.end) {
        start = r.start
      }

      let remove = 0

      for (let j = i; j < this.ranges.length; j++) {
        const n = this.ranges[j]
        if (n.start > end) break
        if (n.start <= end && n.end > end) end = n.end
        remove++
      }

      this.ranges.splice(i, remove, { start, end })
      return
    }

    if (r !== null) {
      if (start <= r.end && end > r.end) {
        r.end = end
        return
      }

      // we never
      if (r.end > start) return
    }

    this.ranges.push({ start, end })
  }

  flush (writer) {
    for (const { start, end } of this.ranges) {
      let index = start

      while (index < end) {
        const page = this.bitfield.getPage(index, this.drop === false)

        const buf = b4a.allocUnsafe(page.bitfield.byteLength)
        buf.set(page.bitfield)

        const last = (page.index + 1) * (buf.byteLength << 3)
        const stop = end < last ? end : last

        while (index < stop) {
          const byte = (index >> 3) - (page.index * buf.byteLength)
          const bit = index & 7

          const mask = 0b1 << bit

          buf[byte] = this.drop
            ? buf[byte] & (mask & 0xff)
            : buf[byte] | mask

          writer.putBitfieldPage(page.index, buf)

          index++
        }
      }
    }

    return {
      ranges: this.ranges,
      drop: this.drop
    }
  }
}
