const hypercore = require('../')
const path = require('path')

const source = hypercore(path.join(__dirname, 'cores/16kb'))

source.ready(function () {
  const dest = hypercore(path.join(__dirname, 'cores/16kb-copy'), source.key, { overwrite: true })
  const then = Date.now()

  replicate(source, dest).on('end', function () {
    console.log(Math.floor(1000 * dest.byteLength / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * dest.length / (Date.now() - then)) + ' blocks/s')
  })
})

function replicate (a, b) {
  const s = a.replicate(false)
  return s.pipe(b.replicate(true)).pipe(s)
}
