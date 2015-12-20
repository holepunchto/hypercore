var crypto = require('crypto')
var uint64be = require('uint64be')

// https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
var DATA_TYPE = new Buffer([0])
var TREE_TYPE = new Buffer([1])
var ROOT_TYPE = new Buffer([2])

var tmp = new Buffer([0, 0, 0, 0, 0, 0, 0, 0])

exports.data = function (data) {
  return createHash().update(DATA_TYPE).update(data).digest()
}

exports.tree = function (a, b) {
  return createHash().update(TREE_TYPE).update(a).update(b).digest()
}

exports.root = function (roots) {
  var hash = createHash().update(ROOT_TYPE)
  for (var i = 0; i < roots.length; i++) {
    var r = roots[i]
    hash.update(r.hash)
    hash.update(uint64be.encode(r.index, tmp))
  }
  return hash.digest()
}

function createHash () {
  return crypto.createHash('sha256')
}
