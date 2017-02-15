module.exports = function replicate (a, b, opts) {
  var stream = a.replicate(opts)
  stream.pipe(b.replicate(opts)).pipe(stream)
}
