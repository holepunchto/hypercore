// TODO: move to module or integrate with the bitfield module?

module.exports = Bitfield

function Bitfield (buffer) {
  if (!(this instanceof Bitfield)) return new Bitfield(buffer)
  if (!buffer) buffer = 1

  if (typeof buffer === 'number') {
    buffer = Buffer(Math.ceil(buffer / 8))
    buffer.fill(0)
  }

  this.length = buffer.length * 8
  this.buffer = buffer
}

Bitfield.prototype.set = function (index, val) {
  var bit = index & 7
  var byte = (index - bit) / 8
  var mask = 128 >> bit

  if (byte >= this.buffer.length) {
    this.buffer = realloc(this.buffer, byte)
    this.length = this.buffer.length * 8
  }

  var b = this.buffer[byte]
  var n = val ? b | mask : b & ~mask

  if (b === n) return false
  this.buffer[byte] = n
  return true
}

Bitfield.prototype.get = function (index) {
  var bit = index & 7
  var byte = (index - bit) / 8
  if (byte >= this.buffer.length) return false

  return !!(this.buffer[byte] & (128 >> bit))
}

Bitfield.prototype.toBuffer = function (offset) {
  if (!offset) offset = 0
  var end = this.buffer.length - 1
  while (end >= 0 && !this.buffer[end]) end--
  return this.buffer.slice(Math.floor(offset / 8), end + 1)
}

function realloc (buf, index) {
  var length = buf.length
  while (length <= index) length = length === 0 ? 1 : length * 2
  var nbuf = Buffer(length)
  buf.copy(nbuf)
  nbuf.fill(0, buf.length)
  return nbuf
}
