var flat = require('flat-tree')
var rle = require('bitfield-rle')
var pager = require('memory-pager')
var bitfield = require('sparse-bitfield')

var INDEX_UPDATE_MASK = [63, 207, 243, 252]
var INDEX_ITERATE_MASK = [0, 192, 240, 252]
var DATA_ITERATE_MASK = [128, 192, 224, 240, 248, 252, 254, 255]
var DATA_UPDATE_MASK = [127, 191, 223, 239, 247, 251, 253, 254]
var MAP_PARENT_RIGHT = new Array(256)
var MAP_PARENT_LEFT = new Array(256)
var NEXT_DATA_0_BIT = new Array(256)
var NEXT_INDEX_0_BIT = new Array(256)
var TOTAL_1_BITS = new Array(256)

for (var i = 0; i < 256; i++) {
  var a = (i & (15 << 4)) >> 4
  var b = i & 15
  var nibble = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4]
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

  var deduplicate = Buffer.allocUnsafe(pageSize)
  deduplicate.fill(255)

  this.indexSize = pageSize - 2048 - 1024
  this.pages = pager(pageSize, { deduplicate })

  if (pages) {
    for (var i = 0; i < pages.length; i++) {
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
  var o = i & 7
  i = (i - o) / 8
  var v = value ? this.data.getByte(i) | (128 >> o) : this.data.getByte(i) & DATA_UPDATE_MASK[o]

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
  var o = start & 7
  var e = end & 7
  var pos = (start - o) / 8
  var last = (end - e) / 8
  var leftMask = (255 - (o ? DATA_ITERATE_MASK[o - 1] : 0))
  var rightMask = (e ? DATA_ITERATE_MASK[e - 1] : 0)
  var byte = this.data.getByte(pos)
  if (pos === last) {
    return TOTAL_1_BITS[byte & leftMask & rightMask]
  }
  var total = TOTAL_1_BITS[byte & leftMask]
  for (var i = pos + 1; i < last; i++) {
    total += TOTAL_1_BITS[this.data.getByte(i)]
  }
  total += TOTAL_1_BITS[this.data.getByte(last) & rightMask]
  return total
}

// TODO: use the index to speed this up *a lot*
Bitfield.prototype.compress = function (start, length) {
  if (!start && !length) return rle.encode(this.data.toBuffer())

  if (start + length > this.length) length = Math.max(1, this.length - start)
  var buf = Buffer.alloc(Math.ceil(length / 8))

  var p = start / this.data.pageSize / 8
  var end = p + length / this.data.pageSize / 8
  var offset = p * this.data.pageSize

  for (; p < end; p++) {
    var page = this.data.pages.get(p, true)
    if (!page || !page.buffer) continue
    page.buffer.copy(buf, p * this.data.pageSize - offset, this.data.pageOffset, this.data.pageOffset + this.data.pageSize)
  }

  return rle.encode(buf)
}

Bitfield.prototype._setIndex = function (i, value) {
  //                    (a + b | c + d | e + f | g + h)
  // -> (a | b | c | d)                                (e | f | g | h)
  //

  var o = i & 3
  i = (i - o) / 4

  var bitfield = this.index
  var ite = this._iterator
  var start = 2 * i
  var byte = (bitfield.getByte(start) & INDEX_UPDATE_MASK[o]) | (getIndexValue(value) >> (2 * o))
  var len = bitfield.length
  var maxLength = this.pages.length * this.indexSize

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
  var roots = flat.fullRoots(2 * len)
  var bitfield = this.index
  var ite = this._iterator
  var byte = 0

  for (var i = 0; i < roots.length; i++) {
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
  var ite = new Iterator(this)

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

  var o = offset & 7

  this._pos = (offset - o) / 8
  this._byte = this._bitfield.data.getByte(this._pos) | (o ? DATA_ITERATE_MASK[o - 1] : 0)

  return this
}

Iterator.prototype.random = function () {
  var i = this.seek(Math.floor(Math.random() * (this.end - this.start))).next()
  return i === -1 ? this.seek(0).next() : i
}

Iterator.prototype.next = function () {
  if (this._pos === -1) return -1

  var dataBitfield = this._bitfield.data
  var free = NEXT_DATA_0_BIT[this._byte]

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

  var n = 8 * this._pos + free
  return n < this.end ? n : -1
}

Iterator.prototype.peek = function () {
  if (this._pos === -1) return -1

  var free = NEXT_DATA_0_BIT[this._byte]
  var n = 8 * this._pos + free
  return n < this.end ? n : -1
}

Iterator.prototype._skipAhead = function (start) {
  var indexBitfield = this._bitfield.index
  var treeEnd = this._indexEnd
  var ite = this._bitfield._iterator
  var o = start & 3

  ite.seek(2 * ((start - o) / 4))

  var treeByte = indexBitfield.getByte(ite.index) | INDEX_ITERATE_MASK[o]

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

  var free = NEXT_INDEX_0_BIT[treeByte]
  if (free === -1) free = 4

  var next = ite.index * 2 + free

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
