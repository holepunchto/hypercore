module.exports = function replicate (a, b, opts, bOpts) {
  var stream = a.replicate(false, opts)
  return stream.pipe(b.replicate(true, bOpts || opts)).pipe(stream)
}
