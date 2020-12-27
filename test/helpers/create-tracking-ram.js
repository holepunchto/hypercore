const ram = require('random-access-memory')

module.exports = function () {
  const logByFilename = {}
  const factory = function (filename) {
    const memory = ram()
    const log = []
    logByFilename[filename] = log
    return {
      read: logAndForward('read'),
      write: logAndForward('write'),
      del: logAndForward('del')
    }

    function logAndForward (op) {
      return function () {
        const statement = {}
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
