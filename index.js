var subleveldown = require('subleveldown')
var feed = require('./lib/feed')
var swarm = require('./lib/swarm')
var messages = require('./lib/messages')
var writeStream = require('./lib/write-stream')
var readStream = require('./lib/read-stream')
var events = require('events')
var util = require('util')

module.exports = Hypercore

function Hypercore (db, opts) {
  if (!(this instanceof Hypercore)) return new Hypercore(db, opts)
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  this.db = db
  this._hashes = subleveldown(db, 'hashes', {valueEncoding: 'binary'})
  this._blocks = subleveldown(db, 'blocks', {valueEncoding: 'binary'})
  this._bitfields = subleveldown(db, 'bitfields', {valueEncoding: 'binary'})
  this._feeds = subleveldown(db, 'feeds', {valueEncoding: messages.Feed})
  this._cache = subleveldown(db, 'data', {valueEncoding: 'binary'})
  this._storage = opts.storage || null
  this._opened = {}

  this.swarm = swarm(this, opts)
}

util.inherits(Hypercore, events.EventEmitter)

Hypercore.prototype.createPeerStream = function () {
  return this.swarm.createStream()
}

Hypercore.prototype.createWriteStream = function (opts) {
  return writeStream(this, opts)
}

Hypercore.prototype.createReadStream = function (id, opts) {
  return readStream(this, id, opts)
}

Hypercore.prototype.list = function () {
  return this._feeds.createKeyStream()
}

Hypercore.prototype.get = function (link, opts) {
  if (typeof link === 'string') link = new Buffer(link, 'hex')
  if (link.id) {
    if (!opts) opts = link
    link = link.id
  }

  var id = link.toString('hex')
  var fd = this._opened[id]
  if (fd) return fd
  fd = this._opened[id] = feed(this, link, opts)
  if (this.swarm.joined[id]) this.swarm.joined[id].open(fd)
  this.emit('interested', fd.id)
  return fd
}

Hypercore.prototype.add = function (opts) {
  return feed(this, null, opts)
}

Hypercore.prototype.use = function (extension) {
  this.swarm.use(extension)
}

Hypercore.prototype._close = function (link) {
  var id = link.toString('hex')
  delete this._opened[id]
  this.emit('uninterested', link)
}
