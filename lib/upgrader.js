module.exports = class Upgrader {
  constructor (session, hooks = {}) {
    this._session = session

    this._fork = null
    this._length = null
    this._latest = null

    this._preupgrade = hooks.preupgrade || null
    this._preupgrading = null
  }

  async onupgrade (request) {
    if (this._preupgrade === null) return request

    const session = this._session

    this._fork = session.snapshotted ? session._snapshot.fork : session.core.tree.fork
    this._length = session.snapshotted ? session._snapshot.length : session.core.tree.length

    session._updateSnapshot()

    const upgraded = await request
    if (!upgraded) return false

    if (this._fork !== session.core.tree.fork) return session._updateSnapshot()

    let preupgrading = this._preupgrading
    if (preupgrading === null) {
      const latest = this._latest = session.session()

      preupgrading = this._preupgrading = Promise.resolve(this._preupgrade(latest))
      preupgrading
        .catch(noop)
        .then(() => latest.close())
    }

    const length = await preupgrading

    this._preupgrading = null

    if (typeof length === 'number' && length >= this._length && length < session.core.tree.length) {
      session._snapshot = {
        length,
        byteLength: 0,
        fork: this._fork,
        compatLength: length
      }

      return length !== this._length
    }

    return session._updateSnapshot()
  }

  ontruncate () {
    // TODO: handle truncation
  }
}

function noop () {}
