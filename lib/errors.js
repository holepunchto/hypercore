module.exports = class HypercoreError extends Error {
  constructor (msg, code) {
    super(msg)
    this.code = code
  }

  static BAD_ARGUMENT (msg) {
    return new HypercoreError(msg, 'BAD_ARGUMENT')
  }

  static STORAGE_EMPTY (msg) {
    return new HypercoreError(msg, 'STORAGE_EMPTY')
  }

  static STORAGE_CONFLICT (msg) {
    return new HypercoreError(msg, 'STORAGE_CONFLICT')
  }

  static INVALID_SIGNATURE (msg) {
    return new HypercoreError(msg, 'INVALID_SIGNATURE')
  }

  static INVALID_CAPABILITY (msg) {
    return new HypercoreError(msg, 'INVALID_CAPABILITY')
  }

  static SNAPSHOT_NOT_AVAILABLE (msg = 'Snapshot is not available') {
    return new HypercoreError(msg, 'SNAPSHOT_NOT_AVAILABLE')
  }

  static REQUEST_CANCELLED (msg = 'Request was cancelled') {
    return new HypercoreError(msg, 'REQUEST_CANCELLED')
  }

  static SESSION_NOT_WRITABLE (msg = 'Session is not writable') {
    return new HypercoreError(msg, 'SESSION_NOT_WRITABLE')
  }

  static SESSION_CLOSED (msg = 'Session is closed') {
    return new HypercoreError(msg, 'SESSION_CLOSED')
  }
}
