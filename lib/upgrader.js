const Snapshot = require('./snapshot')

module.exports = class Upgrader {
  constructor (session, hooks = {}) {
    this.session = session
    this.active = null
    this.preupgrade = hooks.preupgrade || null
  }

  cancel () {
    if (this.active) {
      this.active.cancel()
      this.active = null
    }
  }

  onupgrade (request) {
    if (this.active) this.active.cancel()

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
    const session = this.upgrader.session
    const preupgradeLength = session.length

    if (this.upgrader.preupgrade) session._updateSnapshot()

    let upgraded = await request

    if (!this.active) return this.done(false)

    if (this.upgrader.preupgrade) {
      const latest = session.session()

      let length
      try {
        length = await this.upgrader.preupgrade(latest)
      } catch {
        length = session.length
      } finally {
        await latest.close()
      }

      if (!this.active) return this.done(false)

      if (typeof length === 'number' && length >= preupgradeLength && length <= session.core.tree.length) {
        session._snapshot = new Snapshot(length, 0, session.core.tree.fork)

        return this.done(length !== preupgradeLength)
      }
    }

    if (!session.sparse) {
      // Download all available blocks in non-sparse mode
      const start = session.length
      const end = session.core.tree.length
      const contig = session.contiguousLength

      await session.download({ start, end, ifAvailable: true }).downloaded()

      if (!upgraded) upgraded = session.contiguousLength !== contig
    }

    if (!upgraded) return this.done(false)

    if (this.upgrader.preupgrade || session.snapshotted) return this.done(session._updateSnapshot())

    return this.done(true)
  }
}
