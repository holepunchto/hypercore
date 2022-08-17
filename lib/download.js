module.exports = class Download {
  constructor (req) {
    this.req = req
  }

  async done () {
    return (await this.req).promise
  }

  /**
   * Deprecated. Use `range.done()`.
   */
  downloaded () {
    return this.done()
  }

  destroy () {
    this.req.then(req => req.context && req.context.detach(req), noop)
  }
}

function noop () {}
