var hypercore = require('hypercore')
var memdb = require('memdb')

var core1 = hypercore(memdb())
var core2 = hypercore(memdb())

var feed1 = core1.createFeed()
var feed2 = core2.createFeed(feed1.key)

feed2.get(0, console.log)
feed1.get(0, console.log)

feed2.on('download', function (block, data) {
  console.log('downloaded', block, data)
})

feed1.append('hello')

setTimeout(function () {
  feed1.append('world')
}, 1000)
