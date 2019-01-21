// buffer-equals, but handle 'null' buffer parameters.
module.exports = function safeBufferEquals (a, b) {
  if (!a) return !b
  if (!b) return !a
  return Buffer.compare(a, b) === 0
}
