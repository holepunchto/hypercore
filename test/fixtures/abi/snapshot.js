// Generate an ABI snapshot for the current version of Hypercore.

const path = require('path')
const crypto = require('hypercore-crypto')
const Hypercore = require('../../../')

const { version } = require('../../../package.json')

const core = new Hypercore(path.join(__dirname, `v${version}`), {
  keyPair: crypto.keyPair() // Use an ephemeral key pair
})

core.ready().then(
  async () => {
    for (let i = 0; i < 1000; i++) {
      await core.append(Buffer.from([i]))
    }
  },
  (err) => {
    console.error(err)
    process.exit(1)
  }
)
