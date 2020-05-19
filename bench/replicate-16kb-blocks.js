var hypercore = require('../')
var path = require('path')

var source = hypercore(path.join(__dirname, 'cores/16kb'))

source.ready(function () {
  var dest = hypercore(path.join(__dirname, 'cores/16kb-copy'), source.key, { overwrite: true })
  var then = Date.now()

  replicate(source, dest).on('end', function () {
    console.log(Math.floor(1000 * dest.byteLength / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * dest.length / (Date.now() - then)) + ' blocks/s')
  })
})

function replicate (a, b) {
  var s = a.replicate(false)
  return s.pipe(b.replicate(true)).pipe(s)
}
