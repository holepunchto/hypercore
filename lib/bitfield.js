var bits = require('bit-encode')
var rle = require('bitfield-rle')

/*
FORMAT:
  page1+page2+...

  page -> (4kb, 8096 blocks indexable)
    1kb <data-bits>
    1kb <index-bits>
    2kb <tree-bits>
*/

var BLANK = alloc()

module.exports = Bitfield

function Bitfield (buffer) {
  if (!(this instanceof Bitfield)) return new Bitfield(buffer)

  var len = 16384
  var blen = Math.ceil(buffer ? buffer.length / 4096 : 0)
  while (len < 4194304 && blen > len) len *= 2

  this.length = 0
  this.bigPages = null
  this.pages = new Array(len)
  this.updates = []
  this.tree = new Tree(this)

  if (buffer) {
    for (var i = 0; i < buffer.length; i += 4096) {
      var slice = expand(buffer.slice(i, i + 4096))
      // TODO: also compress if it is full
      if (!compressable(slice, BLANK)) this._getPage(i / 4096, true, slice)
    }
  }
}

Bitfield.prototype.set = function (index, value) {
  return this._setData(index, value)
}

Bitfield.prototype.get = function (index, value) {
  return this._getData(index)
}

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

Bitfield.prototype._setData = set(1023, 1024, 0)
Bitfield.prototype._getData = get(1023, 1024, 0)
Bitfield.prototype._setIndex = set(1023, 1024, 8192)
Bitfield.prototype._getIndex = get(1023, 1024, 8192)
Bitfield.prototype._setTree = set(2047, 2048, 16384)
Bitfield.prototype._getTree = get(2047, 2048, 16384)

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

  var len = (n + 1) * 8192 + offset
  if (len > this.length) {
    this.length = len
    this.tree.length = 2 * len
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

function Tree (bitfield) {
  this.bitfield = bitfield
  this.length = 0
}

Tree.prototype.set = function (index, value) {
  return this.bitfield._setTree(index, value)
}

Tree.prototype.get = function (index) {
  return this.bitfield._getTree(index)
}

function set (mask, size, bitOffset) {
  return function (index, value) {
    var bytesIndex = (index - (index & 7)) / 8
    var bytesOffset = bytesIndex - (bytesIndex & mask)
    var page = this._getPage(bytesOffset / size, true, null)

    if (!bits.set(page.buffer, index - 8 * bytesOffset + bitOffset, value)) return false

    if (!page.updated) {
      page.updated = true
      this.updates.push(page)
    }

    return true
  }
}

function get (mask, size, bitOffset) {
  return function (index) {
    var bytesIndex = (index - (index & 7)) / 8
    var bytesOffset = bytesIndex - (bytesIndex & mask)
    var page = this._getPage(bytesOffset / size, false, null)

    if (!page) return false
    return bits.get(page.buffer, index - 8 * bytesOffset + bitOffset)
  }
}

function Page (buffer, n, offset) {
  this.offset = 4096 * n + offset
  this.updated = false
  this.buffer = buffer
}

function alloc () {
  if (Buffer.alloc) return Buffer.alloc(4096)
  var b = new Buffer(4096)
  b.fill(0)
  return b
}

function compressable (buf, target) {
  return !!(buf.equals && buf.equals(target))
}

function expand (buf) {
  if (buf.length === 4096) return buf
  var b = alloc()
  buf.copy(b)
  return b
}
