module.exports = function replicate (a, b, opts) {
  var stream = a.replicate(opts)
  return stream.pipe(b.replicate(opts)).pipe(stream)
}
