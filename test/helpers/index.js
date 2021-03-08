const Hypercore = require('../../')
const ram = require('random-access-memory')

module.exports = {
  async create (...args) {
    const core = new Hypercore(ram, ...args)
    await core.ready()
    return core
  },

  replicate (a, b) {
    const s1 = a.replicate(true)
    const s2 = b.replicate(false)
    s1.pipe(s2).pipe(s1)
    return [s1, s2]
  }
}
