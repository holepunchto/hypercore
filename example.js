// var bitfield = require('./lib/bitfield')

// var bits = bitfield(16 * 1024 * 8 * 10)

// for (var i = 0;i < bits.length; i++) {
//   bits.set(i, true)
// }

// for (var i = 0; i < bits.length; i += 40) {
//   // var end = i + 20 + ((Math.random() * 20) | 0)
//   // for (var j = i; j < end; j++) {
//   //   bits.set(j, true)
//   // }
// }

// function gzip (bits) {
//   var zlib = require('zlib')
//   return zlib.gzipSync(bits.buffer)
// }

// console.log(16 * 1024 / 8)
// console.log(bits.buffer.length)
// console.log(gzip(bits).length)

var hypercore = require('hypercore')
var memdb = require('memdb')

var core1 = hypercore(memdb())
var core2 = hypercore(memdb())

var feed1 = core1.createFeed()
var feed2 = core2.createFeed(feed1.key)

feed2.get(0, console.log)
feed2.get(1, console.log)

var stream = feed2.replicate()
stream.pipe(feed1.replicate()).pipe(stream)

feed1.append(['hello'])

setTimeout(function () {
  feed1.append('world')
}, 1000)
