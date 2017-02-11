var hypercore = require('../')
var raf = require('random-access-file')
var shuffle = require('shuffle-array')
var path = require('path')

var source = hypercore(data('64kb'))

source.ready(function () {
  var dest = hypercore(source.key, {storage: data('64kb-copy'), reset: true})

  var then = Date.now()
  var missing = []
  var active = 0
  var size = 0
  var cnt = 0

  for (var i = 0; i < source.blocks; i++) missing.push(i)

  shuffle(missing)

  for (var j = 0; j < 16; j++) {
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

    var block = missing.shift()

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

function data (folder) {
  return function (name) {
    return raf(path.join(__dirname, 'cores', folder, name))
  }
}
