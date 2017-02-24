var bitfield = require('sparse-bitfield')
// var andTree = require('bit-and-tree')

module.exports = Selection
module.exports.Range = Range

function Selection (feed) {
  if (!(this instanceof Selection)) return new Selection(feed)

  this.reserved = bitfield()
  this.tree = null
  this.feed = feed
}

Selection.prototype.add = function () {

}

Selection.prototype.next = function () {

}

function Range (feed, start, end) {
  this.bitfield = feed.bitfield
  this.start = start
  this.end = end
  this.trees = null
  this.resize(start, end)
}

Range.prototype.random = function () {
  var tree = this._pickTree((Math.random() * this.trees.length) | 0)
  if (!tree) return -1
  return this._random(tree.left, tree.tree, tree.right)
}

Range.prototype.next = function (offset) {
  if (offset < this.start) offset = this.start
  return this._visit(offset, false)
}

Range.prototype.linear = function () {
  var tree = this._pickTree(0)
  if (!tree) return -1
  return this._linear(tree.left, tree.tree, tree.right)
}

Range.prototype.resize = function (start, end) {
  this.trees = []
  this.start = start
  this.end = end
  this._visit(start, true)
  this.bitfield.set(end - 1, this.bitfield.get(end - 1)) // hack
}

Range.prototype._run = function (init, start, tree, next) {
  if (init) {
    this._nextTree(start, tree, next - 2)
    return -1
  }
  if (this.bitfield.queryIndex(tree) !== 1) return this._linear(start, tree, next - 2)
  return -1
}

// factor a range [start, end[ into a series of peaks in flat-tree notation
Range.prototype._visit = function (offset, init) {
  var start = offset * 2
  var end = this.end * 2

  if (end <= start) return -1

  var rem = start / 2
  var skip = 1
  var next = 0

  var result = -1

  if (rem) {
    do {
      if (!(rem & 1)) {
        do {
          skip *= 2
          rem /= 2
        }
        while (rem && !(rem & 1))
      }

      next = start + 2 * skip
      if (next >= end) break

      result = this._run(init, start, start + skip - 1, next)
      if (result > -1) return result
      start = next
    } while (++rem !== 2)

    while (true) {
      next = start + 4 * skip
      if (next >= end) break

      skip *= 2
      result = this._run(init, start, start + skip - 1, next)
      if (result > -1) return result
      start = next
    }
  } else {
    while (start + 2 * skip <= end) skip *= 2
  }

  while (true) {
    next = start + 2 * skip
    while (next > end) {
      if (skip === 1) return -1
      skip /= 2
      next = start + 2 * skip
    }

    result = this._run(init, start, start + skip - 1, next)
    if (result > -1) return result
    start = next
  }
}

Range.prototype._linear = function (l, t, r) {
  while (l !== r) {
    var lt = (l + t - 1) / 2

    switch (this.bitfield.queryIndex(lt)) {
      case -1:
        t = lt
        r = 2 * t - l
        break

      case 0: return l / 2

      case 1:
        t = (t + r + 1) / 2
        l = 2 * t - r
        break
    }
  }

  return l / 2
}

Range.prototype._random = function (l, t, r) {
  while (l !== r) {
    if (Math.random() < 0.5) {
      var lt = (l + t - 1) / 2

      switch (this.bitfield.queryIndex(lt)) {
        case -1:
          t = lt
          r = 2 * t - l
          break

        case 0: return getRandom(l / 2, lt - l / 2)

        case 1:
          t = (t + r + 1) / 2
          l = 2 * t - r
          break
      }
    } else {
      var rt = (t + r + 1) / 2

      switch (this.bitfield.queryIndex(rt)) {
        case -1:
          t = rt
          l = 2 * t - r
          break

        case 0: return getRandom(rt - r / 2, r / 2)

        case 1:
          t = (l + t - 1) / 2
          r = 2 * t - l
          break
      }
    }
  }

  return l / 2
}

Range.prototype._pickTree = function (offset) {
  var t = null

  for (var i = 0; i < this.trees.length; i++) {
    if (offset === this.trees.length) offset = 0

    t = this.trees[offset++]
    if (this.bitfield.queryIndex(t.tree) !== 1) return t

    this.trees.splice(--offset, 1)
    i--
  }

  return null
}

Range.prototype._nextTree = function (l, t, r) {
  this.trees.push({
    left: l,
    tree: t,
    right: r
  })
}

function getRandom (l, r) {
  if (l === r) return l
  return Math.round(Math.random() * (r - l)) + l
}
