const COMPAT = true // should be flipped once this is widely deployed
const BATCH_UINTS = COMPAT ? 65536 : 128
const BATCH_BYTES = BATCH_UINTS * 4
const BATCH = BATCH_BYTES * 8 // in bits
const MAX_REMOTE_BATCHES = 4
const MAX_RANGE = 8 * 256 * 1024
const MIN_RANGE = 32 // 4bit ints
const MAX = 512
const MAX_ANY = 16

class LocalWants {
  constructor(peer) {
    this.destroyed = false
    this.peer = peer
    this.wants = new Map()
    this.free = new Set()
    this.any = null
  }

  add(index, handle) {
    const b = (index - (index & (BATCH - 1))) / BATCH
    return this.addBatch(b, handle)
  }

  addAnyRange(start, length, handle) {
    if (this.destroyed) return null
    if (this.any === null) this.any = []

    if (COMPAT) {
      let r = start & (BATCH - 1)
      start -= r
      length += r
      r = length & (BATCH - 1)
      if (r) length = length - r + BATCH
    }

    for (let i = 0; i < this.any.length; i++) {
      const w = this.any[i]
      if (w.start === start && w.length === length) {
        w.handles.add(handle)
        handle.addWant(w)
        return null
      }
    }

    const w = { wants: this, start, length, any: true, handles: new Set([handle]) }
    this.any.push(w)
    handle.addWant(w)

    return { start, length, any: !COMPAT }
  }

  removeAnyRange(start, length, handle) {
    if (this.any === null) return null

    for (let i = 0; i < this.any.length; i++) {
      const w = this.any[i]
      if (w.start !== start || w.length !== length) continue

      w.handles.delete(handle)
      handle.removeWant(w)
      if (w.handles.size > 0) return null

      const head = this.any.pop()
      if (head !== w) this.any[i] = head
      return { start, length, any: !COMPAT }
    }

    return null
  }

  addBatch(index, handle) {
    if (this.destroyed) return null
    let w = this.wants.get(index)

    if (w) {
      const size = w.handles.size
      w.handles.add(handle)
      if (w.handles.size !== size) {
        handle.addWant(w)
      }
      return null
    }

    // start here is the batch number for simplicity....
    w = { wants: this, start: index, length: 0, any: false, handles: new Set([handle]) }

    if (this.free.has(index)) {
      this.free.delete(index)
      this.wants.set(index, w)
      handle.addWant(w)
      return null
    }

    let unwant = null
    if (this.wants.size + this.free.size === MAX) {
      if (this.free.size === 0) {
        return null
      }
      unwant = this._unwant()
    }

    this.wants.set(index, w)
    handle.addWant(w)

    return {
      want: { start: index * BATCH, length: BATCH, any: false },
      unwant
    }
  }

  remove(index, handle) {
    const b = (index - (index & (BATCH - 1))) / BATCH
    return this.removeBatch(b, handle)
  }

  removeBatch(index, handle) {
    if (this.destroyed) return false

    const w = this.wants.get(index)
    if (!w) {
      return false
    }

    w.handles.delete(handle)
    handle.removeWant(w)

    if (w.handles.size === 0) {
      this.free.add(index)
      this.wants.delete(index)
      return this.free.size === 1 && this.wants.size === MAX - 1
    }

    return false
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true

    for (const w of this.wants.values()) {
      for (const handle of w.handles) {
        handle.removeWant(w)
      }
    }

    this.wants.clear()
  }

  _unwant() {
    for (const index of this.free) {
      this.free.delete(index)
      return { start: index * BATCH, length: BATCH, any: false }
    }
    return null
  }
}

class RemoteWants {
  constructor() {
    this.any = null
    this.all = false
    this.size = 0
    this.batches = []
  }

  hasAny(start, length) {
    for (let i = 0; i < this.any.length; i++) {
      const a = this.any[i]
      const e = start + length
      const end = a.start + a.length

      if (a.start <= start && start < end) return true
      if (a.start < e && e <= end) return true
    }

    return this.all
  }

  hasRange(start, length) {
    if (this.any !== null && this.hasAny(start, length)) return true
    if (length === 1) return this.has(start)

    let smallest = -1

    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      if (b.length < smallest || smallest === -1) smallest = b.length
    }

    if (smallest === -1) return this.all

    const r = start & (smallest - 1)
    const end = start + length

    let max = 3

    for (let i = start - r; i < end; i += smallest) {
      if (max-- === 0) return true // just to save work
      if (this.has(i)) return true
    }

    return this.all
  }

  has(index) {
    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      const block = (index - (index & (b.length - 1))) / b.length
      if (b.ranges.has(block)) return true
    }

    return this.all
  }

  add(range) {
    if (range.any) {
      if (this.any === null) this.any = []
      if (this.any.length < MAX_ANY) this.any.push(range)
      else this.all = true
      return true
    }

    if (range.length > MAX_RANGE) return false

    if (!validateBatchRange(range)) {
      this.all = true
      return true
    }

    if (this.size >= MAX) {
      this.all = true
      return true
    }

    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      if (b.length === range.length) {
        b.ranges.add(range.start / range.length)
        this.size++
        return true
      }
    }

    if (this.batches.length >= MAX_REMOTE_BATCHES) {
      this.all = true
      return true
    }

    this.batches.push({
      length: range.length,
      ranges: new Set([range.start / range.length])
    })
    this.size++

    return true
  }

  remove(range) {
    if (range.any) {
      if (this.any === null) return false

      for (let i = 0; i < this.any.length; i++) {
        const a = this.any[i]

        if (a.start === range.start && a.length === range.length) {
          const head = this.any.pop()
          if (head !== a) this.any[i] = head
          return true
        }
      }

      return false
    }

    if (!validateBatchRange(range)) return false

    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      if (b.length !== range.length) continue

      const size = b.ranges.size
      b.ranges.delete(range.start / range.length)
      if (b.ranges.size !== size) this.size--

      if (b.ranges.size === 0) {
        const head = this.batches.pop()
        if (head !== b) this.batches[i] = head
      }

      return true
    }

    return false
  }
}

exports.LocalWants = LocalWants
exports.RemoteWants = RemoteWants
exports.WANT_BATCH = BATCH

function validateBatchRange(range) {
  if (range.length > MAX_RANGE || range.length < MIN_RANGE) {
    return false
  }
  // check if power of two
  if ((range.length & (range.length - 1)) !== 0) {
    return false
  }
  // start must be a multiple of the length
  if (range.start & (range.length - 1)) {
    return false
  }

  return true
}
