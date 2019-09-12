module.exports = function replicate (a, b, opts) {
  var stream = a.replicate(false, opts)
  return stream.pipe(b.replicate(true, opts)).pipe(stream)
}
