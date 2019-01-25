var ram = require('random-access-memory')

module.exports = function () {
  var logByFilename = {}
  var factory = function (filename) {
    var memory = ram()
    var log = []
    logByFilename[filename] = log
    return {
      read: logAndForward('read'),
      write: logAndForward('write'),
      del: logAndForward('del')
    }

    function logAndForward (op) {
      return function () {
        var statement = {}
        statement[op] = [].slice.apply(arguments)
        statement[op].pop()
        log.push(statement)
        return memory[op].apply(memory, arguments)
      }
    }
  }
  factory.log = logByFilename
  return factory
}
