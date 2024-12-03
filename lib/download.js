module.exports = class Download {
  constructor (session, range) {
    this.session = session
    this.range = range
    this.request = null
    this.opened = false
    this.opening = this._open()
    this.opening.catch(noop)
  }

  ready () {
    return this.opening
  }

  async _open () {
    if (this.session.opened === false) await this.session.opening
    this._download()
    this.opened = true
  }

  async done () {
    await this.ready()

    try {
      return await this.request.promise
    } catch (err) {
      if (isSessionMoved(err)) return this._download()
      throw err
    }
  }

  _download () {
    const activeRequests = (this.range && this.range.activeRequests) || this.session.activeRequests
    this.request = this.session.core.replicator.addRange(activeRequests, this.range)
    this.request.promise.catch(noop)
    return this.request.promise
  }

  /**
   * Deprecated. Use `range.done()`.
   */
  downloaded () {
    return this.done()
  }

  destroy () {
    this._destroyBackground().catch(noop)
  }

  async _destroyBackground () {
    if (this.opened === false) await this.ready()
    if (this.request.context) this.request.context.detach(this.request)
  }
}

function noop () {}

function isSessionMoved (err) {
  return err.code === 'SESSION_MOVED'
}
