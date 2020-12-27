const hypercore = require('../')
const shuffle = require('shuffle-array')
const path = require('path')

const source = hypercore(path.join(__dirname, 'cores/64kb'))

source.ready(function () {
  const dest = hypercore(path.join(__dirname, 'cores/64kb-copy'), source.key, { overwrite: true })

  const then = Date.now()
  const missing = []
  let active = 0
  let size = 0
  let cnt = 0

  for (let i = 0; i < source.length; i++) missing.push(i)

  shuffle(missing)

  for (let j = 0; j < 16; j++) {
    active++
    copy(null, null)
  }

  function copy (err, data) {
    if (err) throw err

    active--

    if (!missing.length) {
      if (!active) {
        console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
        console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
      }
      return
    }

    const block = missing.shift()

    active++
    source.proof(block, function (err, proof) {
      if (err) throw err
      source.get(block, function (err, data) {
        if (err) throw err
        size += data.length
        cnt++
        dest.put(block, data, proof, copy)
      })
    })
  }
})
