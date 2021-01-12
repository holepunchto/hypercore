const Omega = require('../../')
const ram = require('random-access-memory')

module.exports = {
  async create (...args) {
    const o = new Omega(ram, ...args)
    await o.ready()
    return o
  },

  replicate (a, b) {
    const s1 = a.replicate()
    const s2 = b.replicate()
    s1.pipe(s2).pipe(s1)
    return [s1, s2]
  }
}
