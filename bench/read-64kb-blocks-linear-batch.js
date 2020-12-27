const path = require('path')
const hypercore = require('../')

const feed = hypercore(path.join(__dirname, 'cores/64kb'))

const then = Date.now()
let size = 0
let cnt = 0

feed.createReadStream({ batch: 100 })
  .on('data', function (data) {
    size += data.length
    cnt++
  })
  .on('end', function () {
    console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
  })
