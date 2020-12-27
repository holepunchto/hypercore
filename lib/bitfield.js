const flat = require('flat-tree')
const rle = require('bitfield-rle')
const pager = require('memory-pager')
const bitfield = require('sparse-bitfield')

const INDEX_UPDATE_MASK = [63, 207, 243, 252]
const INDEX_ITERATE_MASK = [0, 192, 240, 252]
const DATA_ITERATE_MASK = [128, 192, 224, 240, 248, 252, 254, 255]
const DATA_UPDATE_MASK = [127, 191, 223, 239, 247, 251, 253, 254]
const MAP_PARENT_RIGHT = new Array(256)
const MAP_PARENT_LEFT = new Array(256)
const NEXT_DATA_0_BIT = new Array(256)
const NEXT_INDEX_0_BIT = new Array(256)
const TOTAL_1_BITS = new Array(256)

for (let i = 0; i < 256; i++) {
  const a = (i & (15 << 4)) >> 4
  const b = i & 15
  const nibble = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4]
  MAP_PARENT_RIGHT[i] = ((a === 15 ? 3 : a === 0 ? 0 : 1) << 2) | (b === 15 ? 3 : b === 0 ? 0 : 1)
  MAP_PARENT_LEFT[i] = MAP_PARENT_RIGHT[i] << 4
  NEXT_DATA_0_BIT[i] = i === 255 ? -1 : (8 - Math.ceil(Math.log(256 - i) / Math.log(2)))
  NEXT_INDEX_0_BIT[i] = i === 255 ? -1 : Math.floor(NEXT_DATA_0_BIT[i] / 2)
  TOTAL_1_BITS[i] = nibble[i >> 4] + nibble[i & 0x0F]
}

module.exports = Bitfield

function Bitfield (pageSize, pages) {
  if (!(this instanceof Bitfield)) return new Bitfield(pageSize, pages)
  if (!pageSize) pageSize = 2048 + 1024 + 512

  const deduplicate = Buffer.allocUnsafe(pageSize)
  deduplicate.fill(255)

  this.indexSize = pageSize - 2048 - 1024
  this.pages = pager(pageSize, { deduplicate })

  if (pages) {
    for (let i = 0; i < pages.length; i++) {
      this.pages.set(i, pages[i])
    }
  }

  this.data = bitfield({
    pageSize: 1024,
    pageOffset: 0,
    pages: this.pages,
    trackUpdates: true
  })

  this.tree = bitfield({
    pageSize: 2048,
    pageOffset: 1024,
    pages: this.pages,
    trackUpdates: true
  })

  this.index = bitfield({
    pageSize: this.indexSize,
    pageOffset: 1024 + 2048,
    pages: this.pages,
    trackUpdates: true
  })

  this.length = this.data.length
  this._iterator = flat.iterator(0)
}

Bitfield.prototype.set = function (i, value) {
  const o = i & 7
  i = (i - o) / 8
  const v = value ? this.data.getByte(i) | (128 >> o) : this.data.getByte(i) & DATA_UPDATE_MASK[o]

  if (!this.data.setByte(i, v)) return false
  this.length = this.data.length
  this._setIndex(i, v)
  return true
}

Bitfield.prototype.get = function (i) {
  return this.data.get(i)
}

Bitfield.prototype.total = function (start, end) {
  if (!start || start < 0) start = 0
  if (!end) end = this.data.length
  if (end < start) return 0
  if (end > this.data.length) {
    this._expand(end)
  }
  const o = start & 7
  const e = end & 7
  const pos = (start - o) / 8
  const last = (end - e) / 8
  const leftMask = (255 - (o ? DATA_ITERATE_MASK[o - 1] : 0))
  const rightMask = (e ? DATA_ITERATE_MASK[e - 1] : 0)
  const byte = this.data.getByte(pos)
  if (pos === last) {
    return TOTAL_1_BITS[byte & leftMask & rightMask]
  }
  let total = TOTAL_1_BITS[byte & leftMask]
  for (let i = pos + 1; i < last; i++) {
    total += TOTAL_1_BITS[this.data.getByte(i)]
  }
  total += TOTAL_1_BITS[this.data.getByte(last) & rightMask]
  return total
}

// TODO: use the index to speed this up *a lot*
Bitfield.prototype.compress = function (start, length) {
  if (!start && !length) return rle.encode(this.data.toBuffer())

  if (start + length > this.length) length = Math.max(1, this.length - start)
  const buf = Buffer.alloc(Math.ceil(length / 8))

  let p = start / this.data.pageSize / 8
  const end = p + length / this.data.pageSize / 8
  const offset = p * this.data.pageSize

  for (; p < end; p++) {
    const page = this.data.pages.get(p, true)
    if (!page || !page.buffer) continue
    page.buffer.copy(buf, p * this.data.pageSize - offset, this.data.pageOffset, this.data.pageOffset + this.data.pageSize)
  }

  return rle.encode(buf)
}

Bitfield.prototype._setIndex = function (i, value) {
  //                    (a + b | c + d | e + f | g + h)
  // -> (a | b | c | d)                                (e | f | g | h)
  //

  const o = i & 3
  i = (i - o) / 4

  const bitfield = this.index
  const ite = this._iterator
  const start = 2 * i
  let byte = (bitfield.getByte(start) & INDEX_UPDATE_MASK[o]) | (getIndexValue(value) >> (2 * o))
  const len = bitfield.length
  const maxLength = this.pages.length * this.indexSize

  ite.seek(start)

  while (ite.index < maxLength && bitfield.setByte(ite.index, byte)) {
    if (ite.isLeft()) {
      byte = MAP_PARENT_LEFT[byte] | MAP_PARENT_RIGHT[bitfield.getByte(ite.sibling())]
    } else {
      byte = MAP_PARENT_RIGHT[byte] | MAP_PARENT_LEFT[bitfield.getByte(ite.sibling())]
    }
    ite.parent()
  }

  if (len !== bitfield.length) this._expand(len)

  return ite.index !== start
}

Bitfield.prototype._expand = function (len) {
  const roots = flat.fullRoots(2 * len)
  const bitfield = this.index
  const ite = this._iterator
  let byte = 0

  for (let i = 0; i < roots.length; i++) {
    ite.seek(roots[i])
    byte = bitfield.getByte(ite.index)

    do {
      if (ite.isLeft()) {
        byte = MAP_PARENT_LEFT[byte] | MAP_PARENT_RIGHT[bitfield.getByte(ite.sibling())]
      } else {
        byte = MAP_PARENT_RIGHT[byte] | MAP_PARENT_LEFT[bitfield.getByte(ite.sibling())]
      }
    } while (setByteNoAlloc(bitfield, ite.parent(), byte))
  }
}

function setByteNoAlloc (bitfield, i, b) {
  if (8 * i >= bitfield.length) return false
  return bitfield.setByte(i, b)
}

Bitfield.prototype.iterator = function (start, end) {
  const ite = new Iterator(this)

  ite.range(start || 0, end || this.length)
  ite.seek(0)

  return ite
}

function Iterator (bitfield) {
  this.start = 0
  this.end = 0

  this._indexEnd = 0
  this._pos = 0
  this._byte = 0
  this._bitfield = bitfield
}

Iterator.prototype.range = function (start, end) {
  this.start = start
  this.end = end
  this._indexEnd = 2 * Math.ceil(end / 32)

  if (this.end > this._bitfield.length) {
    this._bitfield._expand(this.end)
  }

  return this
}

Iterator.prototype.seek = function (offset) {
  offset += this.start
  if (offset < this.start) offset = this.start

  if (offset >= this.end) {
    this._pos = -1
    return this
  }

  const o = offset & 7

  this._pos = (offset - o) / 8
  this._byte = this._bitfield.data.getByte(this._pos) | (o ? DATA_ITERATE_MASK[o - 1] : 0)

  return this
}

Iterator.prototype.random = function () {
  const i = this.seek(Math.floor(Math.random() * (this.end - this.start))).next()
  return i === -1 ? this.seek(0).next() : i
}

Iterator.prototype.next = function () {
  if (this._pos === -1) return -1

  const dataBitfield = this._bitfield.data
  let free = NEXT_DATA_0_BIT[this._byte]

  while (free === -1) {
    this._byte = dataBitfield.getByte(++this._pos)
    free = NEXT_DATA_0_BIT[this._byte]

    if (free === -1) {
      this._pos = this._skipAhead(this._pos)
      if (this._pos === -1) return -1

      this._byte = dataBitfield.getByte(this._pos)
      free = NEXT_DATA_0_BIT[this._byte]
    }
  }

  this._byte |= DATA_ITERATE_MASK[free]

  const n = 8 * this._pos + free
  return n < this.end ? n : -1
}

Iterator.prototype.peek = function () {
  if (this._pos === -1) return -1

  const free = NEXT_DATA_0_BIT[this._byte]
  const n = 8 * this._pos + free
  return n < this.end ? n : -1
}

Iterator.prototype._skipAhead = function (start) {
  const indexBitfield = this._bitfield.index
  const treeEnd = this._indexEnd
  const ite = this._bitfield._iterator
  const o = start & 3

  ite.seek(2 * ((start - o) / 4))

  let treeByte = indexBitfield.getByte(ite.index) | INDEX_ITERATE_MASK[o]

  while (NEXT_INDEX_0_BIT[treeByte] === -1) {
    if (ite.isLeft()) {
      ite.next()
    } else {
      ite.next()
      ite.parent()
    }

    if (rightSpan(ite) >= treeEnd) {
      while (rightSpan(ite) >= treeEnd && isParent(ite)) ite.leftChild()
      if (rightSpan(ite) >= treeEnd) return -1
    }

    treeByte = indexBitfield.getByte(ite.index)
  }

  while (ite.factor > 2) {
    if (NEXT_INDEX_0_BIT[treeByte] < 2) ite.leftChild()
    else ite.rightChild()

    treeByte = indexBitfield.getByte(ite.index)
  }

  let free = NEXT_INDEX_0_BIT[treeByte]
  if (free === -1) free = 4

  const next = ite.index * 2 + free

  return next <= start ? start + 1 : next
}

function rightSpan (ite) {
  return ite.index + ite.factor / 2 - 1
}

function isParent (ite) {
  return ite.index & 1
}

function getIndexValue (n) {
  switch (n) {
    case 255: return 192
    case 0: return 0
    default: return 64
  }
}
