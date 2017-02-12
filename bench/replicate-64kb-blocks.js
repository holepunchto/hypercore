var hypercore = require('../')
var path = require('path')

var source = hypercore(path.join(__dirname, 'cores/64kb'))

source.ready(function () {
  var dest = hypercore(path.join(__dirname, 'cores/64kb-copy'), source.key, {overwrite: true})

  replicate(source, dest)

  dest.get(0, function () {
    var then = Date.now()
    dest.download(function () {
      console.log(Math.floor(1000 * dest.byteLength / (Date.now() - then)) + ' bytes/s')
      console.log(Math.floor(1000 * dest.length / (Date.now() - then)) + ' blocks/s')
    })
  })
})

function replicate (a, b) {
  var s = a.replicate()
  s.pipe(b.replicate()).pipe(s)
}
