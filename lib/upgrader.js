module.exports = class Upgrader {
  constructor (session, hooks = {}) {
    this.session = session
    this.active = null
    this.preupgrade = hooks.preupgrade || noop
  }

  cancel () {
    if (this.active) {
      this.active.cancel()
      this.active = null
    }
  }

  async onupgrade (request) {
    if (this.active) return this.active.upgraded

    const upgrade = this.active = new Upgrade(this)
    upgrade.run(request)

    return this.active.upgraded
  }

  ontruncate () {
    // TODO: handle truncation
  }
}

class Upgrade {
  constructor (upgrader) {
    this.upgrader = upgrader
    this.active = true
    this.upgraded = new Promise((resolve) => {
      this.done = resolve
    })
  }

  cancel () {
    this.active = false
  }

  async run (request) {
    if (!this.active) return this.done(false)

    const session = this.upgrader.session

    const preupgradeLength = session.length

    session._updateSnapshot()

    const upgraded = await request
    if (!upgraded || !this.active) return this.done(false)

    const latest = session.session()

    let length
    try {
      length = await this.upgrader.preupgrade(latest)
    } catch {
      length = session.core.tree.length
    } finally {
      await latest.close()
    }

    if (!this.active) return this.done(false)

    if (typeof length === 'number' && length >= preupgradeLength && length < session.core.tree.length) {
      session._snapshot = {
        length,
        byteLength: 0,
        fork: session.core.tree.fork,
        compatLength: length
      }

      return this.done(length !== this._length)
    }

    return this.done(session._updateSnapshot())
  }
}

function noop () {}
