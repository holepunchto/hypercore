var bitfield = require('./bitfield')
var unordered = require('unordered-set')

module.exports = Selection

function Selection (feed) {
  if (!(this instanceof Selection)) return new Selection(feed)
  this._feed = feed
  this._reserved = bitfield()
  this._ranges = [[], [], [], [], []]
  this._offsets = [0, 0, 0, 0, 0]
}

Selection.prototype.next = function (remoteBitfield, end) {
  for (var i = this._ranges.length - 1; i >= 0; i--) {
    var list = this._ranges[i]
    if (!list.length) continue
    if (this._offsets[i] >= list.length) this._offsets[i] = 0

    var offset = this._offsets[i]++

    for (var j = 0; j < list.length; j++) {
      var range = offset + j < list.length ? list[offset + j] : list[offset + j - list.length]
      var blk = this._next(range, remoteBitfield, end)
      if (blk > -1) {
        this._reserved.set(blk, true)
        return {
          block: blk,
          bytes: range.bytes,
          nodes: this._feed.digest(blk)
        }
      }
    }
  }

  return null
}

Selection.prototype.cancel = function (blk) {
  this._reserved.set(blk, false)
}

Selection.prototype.add = function (opts, cb) {
  var prio = toPriority(opts.priority)
  var list = this._ranges[prio]
  var range = new Range(opts, prio, cb)

  unordered.add(list, range)
  return range
}

Selection.prototype.range = function (prio) {
  return this._ranges[prio]
}

Selection.prototype.update = function (block) {
  for (var i = 0; i < this._ranges.length; i++) {
    var r = this._ranges[i]
    var completed = null

    for (var j = 0; j < r.length; j++) {
      var next = r[j]

      while (this._feed.has(next._downloaded) && next._downloaded < next.end) {
        next._downloaded++
      }
      if (next._downloaded === next.end) {
        if (!completed) completed = []
        completed.push(next)
      }
    }

    if (completed) {
      for (var k = 0; k < completed.length; k++) {
        var cb = completed[k].callback
        unordered.remove(r, completed[k])
        if (cb) cb()
      }
    }
  }
}

Selection.prototype.remove = function (opts) {
  if (opts instanceof Range) return !!unordered.remove(this._ranges[opts.priority], opts)

  var prio = toPriority(opts.priority)
  var list = this._ranges[prio]
  var range = new Range(opts, prio)

  for (var i = 0; i < list.length; i++) {
    var next = list[i]
    if ((next.bytes && next.bytes === range.bytes) || (next.start === range.start && next.end === range.end)) {
      unordered.remove(list, next)
      return true
    }
  }

  return false
}

Selection.prototype._next = function (range, remoteBitfield, end) {
  switch (range.strategy) {
    case 0: return this._nextRandom(range, remoteBitfield, end)
    case 1: return this._nextLinear(range, remoteBitfield, end)
  }
}

Selection.prototype._nextLinear = function (range, remoteBitfield, end) {
  end = Math.min(range.end, end)

  for (var i = range._downloaded; i < end; i++) {
    if (this._valid(i, remoteBitfield)) return i
  }

  return -1
}

Selection.prototype._nextRandom = function (range, remoteBitfield, end) {
  end = Math.min(range.end, end)
  if (end < range._downloaded) return -1

  var offset = Math.floor(Math.random() * (end - range._downloaded)) + range._downloaded
  var i = 0

  for (i = offset; i < end; i++) {
    if (this._valid(i, remoteBitfield)) return i
  }
  for (i = range.start; i < offset; i++) {
    if (this._valid(i, remoteBitfield)) return i
  }

  return -1
}

Selection.prototype._valid = function (i, remoteBitfield) {
  return remoteBitfield.get(i) && !this._reserved.get(i) && !this._feed.has(i)
}

function toPriority (p) {
  return Math.min(typeof p === 'number' ? p : 2, 4)
}

function Range (opts, prio, cb) {
  if (typeof opts.block === 'number') {
    this.start = opts.block
    this.end = opts.block + 1
    this.strategy = 1
  } else {
    this.start = opts.start || 0
    this.end = opts.end > 0 ? opts.end : Infinity
    // TODO: re-add me when bisect is enabled
    // this.strategy = opts.bisect ? 2 : opts.linear ? 1 : opts.bytes ? 2 : 0
    this.strategy = opts.linear ? 1 : 0
  }

  this.bytes = opts.bytes || 0
  this.priority = prio
  this.callback = cb

  this._downloaded = this.start
  this._index = 0
}
