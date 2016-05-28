module.exports = function copy (blk, from, to, cb) {
  from.proof(blk, {digest: to.digest(blk)}, function (err, proof) {
    if (err) return cb(err)
    from.get(blk, function (err, data) {
      if (err) return cb(err)
      to.put(blk, data, proof, cb)
    })
  })
}
