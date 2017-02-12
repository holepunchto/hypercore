var path = require('path')
var hypercore = require('../')

var feed = hypercore(path.join(__dirname, 'cores/64kb'))

var then = Date.now()
var size = 0
var cnt = 0

feed.createReadStream()
  .on('data', function (data) {
    size += data.length
    cnt++
  })
  .on('end', function () {
    console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
  })
