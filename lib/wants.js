const BATCH_BYTES = 512
const BATCH = BATCH_BYTES * 8 // in bits
const MAX_REMOTE_BATCHES = 4
const MAX_RANGE = 8 * 256 * 1024
const MIN_RANGE = 32 // 4bit ints
const MAX = 512

class LocalWants {
  constructor(peer) {
    this.destroyed = false
    this.peer = peer
    this.wants = new Map()
    this.free = new Set()
  }

  add(index, handle) {
    const b = (index - (index & (BATCH - 1))) / BATCH
    return this.addBatch(b, handle)
  }

  addBatch(index, handle) {
    if (this.destroyed) return null
    let w = this.wants.get(index)

    if (w) {
      const size = w.handles.size
      w.handles.add(handle)
      if (w.handles.size !== size) handle.addWant(w)
      return null
    }

    w = { wants: this, index, handles: new Set([handle]) }

    if (this.free.has(index)) {
      this.free.delete(index)
      this.wants.set(index, w)
      handle.addWant(w)
      return null
    }

    let unwant = null
    if (this.wants.size + this.free.size === MAX) {
      if (this.free.size === 0) return null
      unwant = this._unwant()
    }

    this.wants.set(index, w)
    handle.addWant(w)

    return {
      want: { start: index * BATCH, length: BATCH },
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
    if (!w) return false

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
      return { start: index * BATCH, length: BATCH }
    }
    return null
  }
}

class RemoteWants {
  constructor() {
    this.batches = []
    this.active = 0
    this.size = 0
  }

  hasRange(start, length) {
    if (length === 1) return this.has(start)

    let smallest = -1

    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      if (b.length < smallest || smallest === -1) smallest = b.length
    }

    if (smallest === -1) return false

    const r = start & (smallest - 1)
    const end = start + length

    let max = 3

    for (let i = start - r; i < end; i += smallest) {
      if (max-- === 0) return true // just to save work
      if (this.has(i)) return true
    }

    return false
  }

  has(index) {
    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      const block = (index - (index & (b.length - 1))) / b.length
      if (b.ranges.has(block)) return true
    }

    return false
  }

  add(range) {
    if (!validateRange(range)) return false
    if (this.size >= MAX) return false

    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      if (b.length === range.length) {
        b.ranges.add(range.start / range.length)
        this.size++
        return true
      }
    }

    if (this.batches.length >= MAX_REMOTE_BATCHES) {
      return false
    }

    this.batches.push({
      length: range.length,
      ranges: new Set([range.start / range.length])
    })

    return true
  }

  remove(range) {
    if (!validateRange(range)) return false

    for (let i = 0; i < this.batches.length; i++) {
      const b = this.batches[i]
      if (b.length !== range.length) continue

      const size = b.ranges.size
      b.ranges.delete(range.start)
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

function validateRange(range) {
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
