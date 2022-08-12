module.exports = class Snapshot {
  constructor (length, byteLength, fork) {
    this.length = length
    this.byteLength = byteLength
    this.fork = fork
    this.compatLength = length
  }
}
