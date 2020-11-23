const Omega = require('../../')
const ram = require('random-access-memory')

module.exports = {
  create () {
    return new Omega(ram)
  }
}
