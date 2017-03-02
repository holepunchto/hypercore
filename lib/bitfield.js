var flat = require('flat-tree')
var rle = require('bitfield-rle')

var INDEX_UPDATE_MASK = [63, 207, 243, 252]
var INDEX_ITERATE_MASK = [0, 192, 240, 252]
var DATA_ITERATE_MASK = [128, 192, 224, 240, 248, 252, 254, 255]
var DATA_UPDATE_MASK = [127, 191, 223, 239, 247, 251, 253, 254]
var MAP_PARENT_RIGHT = new Array(256)
var MAP_PARENT_LEFT = new Array(256)
var NEXT_DATA_0_BIT = new Array(256)
var NEXT_INDEX_0_BIT = new Array(256)
var BLANK = alloc()

for (var i = 0; i < 256; i++) {
  var a = (i & (15 << 4)) >> 4
  var b = i & 15
  MAP_PARENT_RIGHT[i] = ((a === 15 ? 3 : a === 0 ? 0 : 1) << 2) | (b === 15 ? 3 : b === 0 ? 0 : 1)
  MAP_PARENT_LEFT[i] = MAP_PARENT_RIGHT[i] << 4
  NEXT_DATA_0_BIT[i] = i === 255 ? -1 : (8 - Math.ceil(Math.log(256 - i) / Math.log(2)))
  NEXT_INDEX_0_BIT[i] = i === 255 ? -1 : Math.floor(NEXT_DATA_0_BIT[i] / 2)
}

module.exports = Bitfield

function Tree (bitfield) {
  this.bitfield = bitfield
  this.length = 0
}

Tree.prototype.set = function (i, value) {
  var o = i & 7
  i = (i - o) / 8
  var v = value ? this.bitfield._getTreeByte(i) | (128 >> o) : this.bitfield._getTreeByte(i) & DATA_UPDATE_MASK[o]
  return this.bitfield._setTreeByte(i, v, true)
}

Tree.prototype.get = function (i, byte) {
  var o = i & 7
  return !!(this.bitfield._getTreeByte((i - o) / 8) & (128 >> o))
}

function Bitfield (buffer) {
  if (!(this instanceof Bitfield)) return new Bitfield(buffer)

  var len = 16384
  var blen = Math.ceil(buffer ? buffer.length / 3328 : 0)
  while (len < 4194304 && blen > len) len *= 2

  this.length = 0
  this.bigPages = null
  this.pages = new Array(len)
  this.updates = []
  this.tree = new Tree(this)

  this._indexLength = 0
  this._iterator = flat.iterator(0)

  if (buffer) {
    for (var i = 0; i < buffer.length; i += 3328) {
      var slice = expand(buffer.slice(i, i + 3328))
      // TODO: also compress if it is full
      if (!compressable(slice, BLANK)) this._getPage(i / 3328, true, slice)
    }
  }
}

Bitfield.prototype.set = function (i, value) {
  var o = i & 7
  i = (i - o) / 8
  var v = value ? this._getByte(i) | (128 >> o) : this._getByte(i) & DATA_UPDATE_MASK[o]

  if (!this._setByte(i, v, true)) return false
  this._setIndex(i, v)
  return true
}

Bitfield.prototype.get = function (i) {
  var o = i & 7
  return !!(this._getByte((i - o) / 8) & (128 >> o))
}

// TODO: use the index to speed this up *a lot*
Bitfield.prototype.compress = function () {
  var offset = 0
  var byteLength = this.length / 8
  var blank = BLANK.slice(0, 1024)
  var bufs = []

  while (offset < byteLength) {
    var page = this._getPage(offset / 1024, false, null)
    offset += 1024
    bufs.push(page ? page.buffer.slice(0, 1024) : blank)
  }

  return rle.encode(Buffer.concat(bufs))
}

Bitfield.prototype.nextUpdate = function () {
  if (!this.updates.length) return null
  var next = this.updates.pop()
  next.updated = false
  return next
}

Bitfield.prototype._setIndex = function (i, value) {
  //                    (a + b | c + d | e + f | g + h)
  // -> (a | b | c | d)                                (e | f | g | h)
  //

  var o = i & 3
  i = (i - o) / 4

  var ite = this._iterator
  var start = 2 * i
  var byte = (this._getIndexByte(start) & INDEX_UPDATE_MASK[o]) | (getIndexValue(value) >> (2 * o))
  var len = this._indexLength

  ite.seek(start)

  while ((ite.index < this._indexLength || ite.offset) && this._setIndexByte(ite.index, byte, false)) {
    if (ite.isLeft()) {
      byte = MAP_PARENT_LEFT[byte] | MAP_PARENT_RIGHT[this._getIndexByte(ite.sibling())]
    } else {
      byte = MAP_PARENT_RIGHT[byte] | MAP_PARENT_LEFT[this._getIndexByte(ite.sibling())]
    }
    ite.parent()
  }

  if (len !== this._indexLength) this._expand(len)

  return ite.index !== start
}

Bitfield.prototype._expand = function (len) {
  var roots = flat.fullRoots(2 * len)
  var ite = this._iterator
  var byte = 0

  for (var i = 0; i < roots.length; i++) {
    ite.seek(roots[i])
    byte = this._getIndexByte(ite.index)

    do {
      if (ite.isLeft()) {
        byte = MAP_PARENT_LEFT[byte] | MAP_PARENT_RIGHT[this._getIndexByte(ite.sibling())]
      } else {
        byte = MAP_PARENT_RIGHT[byte] | MAP_PARENT_LEFT[this._getIndexByte(ite.sibling())]
      }
    } while (this._setIndexByte(ite.parent(), byte, false))
  }
}

Bitfield.prototype._getByte = get(1023, 1024, 0)
Bitfield.prototype._setByte = set(1023, 1024, 0)
Bitfield.prototype._getTreeByte = get(2047, 2048, 1024)
Bitfield.prototype._setTreeByte = set(2047, 2048, 1024)
Bitfield.prototype._getIndexByte = get(255, 256, 3072)
Bitfield.prototype._setIndexByte = set(255, 256, 3072)

Bitfield.prototype.iterator = function (start, end) {
  var ite = new Iterator(this)

  ite.range(start || 0, end || this.length)
  ite.seek(0)

  return ite
}

// TODO: this needs more testing
Bitfield.prototype._getBigPage = function (n, set, buf) {
  var rem = n & 4194303
  var big = (n - rem) / 4194304

  var pages = this.bigPages[big]
  if (!pages) {
    if (!set) return
    pages = this.bigPages[big] = new Array(4194304)
  }

  return this._page(rem, pages, set, buf, big * 4194304)
}

Bitfield.prototype._getPage = function (n, set, buf) {
  if (!this._grow(n)) return this._getBigPage(n, set, buf)
  return this._page(n, this.pages, set, buf, 0)
}

Bitfield.prototype._page = function (n, pages, set, buf, offset) {
  var p = pages[n]
  if (p || !set) return p
  p = pages[n] = new Page(buf || alloc(), n, offset)

  var len = (n + 1 + offset) * 8192
  if (len > this.length) {
    this.length = len
    this.tree.length = 2 * len
    this._indexLength = len / 32
  }

  return p
}

Bitfield.prototype._grow = function (n) {
  if (!this.pages) return false
  if (n < this.pages.length) return true

  var size = 2 * this.pages.length
  while (size < n) size *= 2

  if (size > 4194304) {
    this.pages = null
    this.bigPages = new Array(1048576)
    return false
  }

  var twice = new Array(size)
  for (var i = 0; i < this.pages.length; i++) twice[i] = this.pages[i]
  this.pages = twice

  return true
}

function Page (buffer, n, offset) {
  this.offset = 3328 * n + offset // 3328 === 1024 + 2048 + 256
  this.updated = false
  this.buffer = buffer
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
  this._byte = this._bitfield._getByte(this._pos) | (o ? DATA_ITERATE_MASK[o - 1] : 0)

  return this
}

Iterator.prototype.random = function () {
  var i = this.seek(Math.floor(Math.random() * (this.end - this.start))).next()
  return i === -1 ? this.seek(0).next() : i
}

Iterator.prototype.next = function () {
  if (this._pos === -1) return -1

  var bitfield = this._bitfield
  var free = NEXT_DATA_0_BIT[this._byte]

  while (free === -1) {
    this._byte = bitfield._getByte(++this._pos)
    free = NEXT_DATA_0_BIT[this._byte]

    if (free === -1) {
      this._pos = this._skipAhead(this._pos)
      if (this._pos === -1) return -1

      this._byte = bitfield._getByte(this._pos)
      free = NEXT_DATA_0_BIT[this._byte]
    }
  }

  this._byte |= DATA_ITERATE_MASK[free]

  var n = 8 * this._pos + free
  return n < this.end ? n : -1
}

Iterator.prototype._skipAhead = function (start) {
  var bitfield = this._bitfield
  var treeEnd = this._indexEnd
  var ite = bitfield._iterator
  var o = start & 3

  ite.seek(2 * ((start - o) / 4))

  var treeByte = bitfield._getIndexByte(ite.index) | INDEX_ITERATE_MASK[o]

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

    treeByte = bitfield._getIndexByte(ite.index)
  }

  while (ite.factor > 2) {
    if (NEXT_INDEX_0_BIT[treeByte] < 2) ite.leftChild()
    else ite.rightChild()

    treeByte = bitfield._getIndexByte(ite.index)
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

function compressable (buf, target) {
  return !!(buf.equals && buf.equals(target))
}

function expand (buf) {
  if (buf.length === 3328) return buf
  var b = alloc()
  buf.copy(b)
  return b
}

function alloc () {
  if (Buffer.alloc) return Buffer.alloc(3328)
  var b = new Buffer(3328)
  b.fill(0)
  return b
}

function set (mask, size, byteOffset) {
  return function (i, byte, grow) {
    var offset = i & mask
    var j = (i - offset) / size
    var page = this._getPage(j, grow, null)

    offset += byteOffset
    if (!page || page.buffer[offset] === byte) return false

    page.buffer[offset] = byte
    if (!page.updated) {
      page.updated = true
      this.updates.push(page)
    }

    return true
  }
}

function get (mask, size, byteOffset) {
  return function (i) {
    var offset = i & mask
    var j = (i - offset) / size
    var page = this._getPage(j, false, null)

    if (!page) return 0
    return page.buffer[offset + byteOffset]
  }
}
