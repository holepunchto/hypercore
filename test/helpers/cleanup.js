var rimraf = require('rimraf')

module.exports = function (feed, t) {
  rimraf(feed._storage.data.directory, function () {
    t.end()
  })
}
