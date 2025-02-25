const b4a = require('b4a')
const quickbit = require('./compat').quickbit

module.exports = class BitInterlude {
  constructor () {
    this.ranges = []
  }

  contiguousLength (from) {
    for (const r of this.ranges) {
      if (r.start > from) break
      if (!r.value && r.start <= from) return r.start
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

      return r.value
    }

    return false
  }

  setRange (start, end, value) {
    if (start === end) return

    let r = null

    for (let i = 0; i < this.ranges.length; i++) {
      r = this.ranges[i]

      // if already inside, stop
      if (r.start <= start && end <= r.end) {
        if (value === r.value) return

        const ranges = mergeRanges(r, { start, end, value })
        this.ranges.splice(i, 1, ...ranges)

        return
      }

      // we wanna overun the interval
      if (start > r.end) {
        continue
      }

      // we overran but this interval is ending after us, move it back
      if (end >= r.start && end <= r.end) {
        r.start = r.value === value ? start : end
        if (r.value !== value) this.ranges.splice(i, 0, { start, end, value })
        return
      }

      // we overran but our start is contained in this interval, move start back
      if (start >= r.start && start <= r.end) {
        if (r.value !== value) {
          this.ranges.splice(++i, 0, { start, end, value })
          r.end = start
          return
        }

        start = r.start
      }

      let remove = 0

      for (let j = i; j < this.ranges.length; j++) {
        const n = this.ranges[j]
        if (n.start > end || n.value !== value) break
        if (n.start <= end && n.end > end) end = n.end
        remove++
      }

      this.ranges.splice(i, remove, { start, end, value })
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

    this.ranges.push({ start, end, value })
  }

  flush (tx, bitfield) {
    if (!this.ranges.length) return []

    let index = this.ranges[0].start
    const final = this.ranges[this.ranges.length - 1].end

    let i = 0

    while (index < final) {
      const page = bitfield.getBitfield(index) // read only
      const pageIndex = page ? page.index : bitfield.getPageIndex(index)

      const buf = b4a.allocUnsafe(bitfield.getPageByteLength())

      if (page) {
        const src = page.bitfield // Uint32Array
        buf.set(b4a.from(src.buffer, src.byteOffset, src.byteLength), 0)
      } else {
        b4a.fill(buf, 0)
      }

      const last = (pageIndex + 1) * (buf.byteLength << 3)
      const offset = pageIndex * (buf.byteLength << 3)

      let hasValue = false

      while (i < this.ranges.length) {
        const { start, end, value } = this.ranges[i]

        if (!hasValue && value) hasValue = true

        const from = start < index ? index : start
        const to = end < last ? end : last

        quickbit.fill(buf, value, from - offset, to - offset)

        index = to

        if (to === last) break

        i++
      }

      if (page || hasValue) tx.putBitfieldPage(pageIndex, buf)
    }

    return this.ranges
  }
}

function mergeRanges (a, b) {
  const ranges = []
  if (a.start < b.start) ranges.push({ start: a.start, end: b.start, value: a.value })
  ranges.push({ start: b.start, end: b.end, value: b.value })
  if (b.end < a.end) ranges.push({ start: b.end, end: a.end, value: a.value })

  return ranges
}
