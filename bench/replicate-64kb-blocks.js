var hypercore = require('../')
var raf = require('random-access-file')
var path = require('path')

var source = hypercore(data('64kb'))

source.ready(function () {
  var dest = hypercore(source.key, {storage: data('64kb-copy'), reset: true})

  replicate(source, dest)

  dest.get(0, function () {
    var then = Date.now()
    dest.download(function () {
      console.log(Math.floor(1000 * dest.bytes / (Date.now() - then)) + ' bytes/s')
      console.log(Math.floor(1000 * dest.blocks / (Date.now() - then)) + ' blocks/s')
    })
  })
})

function replicate (a, b) {
  var s = a.replicate()
  s.pipe(b.replicate()).pipe(s)
}

function data (folder) {
  return function (name) {
    return raf(path.join(__dirname, 'cores', folder, name))
  }
}
