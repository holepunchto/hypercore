const defaultCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const c = require('compact-encoding')
const { BAD_ARGUMENT } = require('hypercore-errors')

const m = require('./messages')
const multisig = require('./multisig')
const caps = require('./caps')

const signatureArray = c.array(c.fixed64)

module.exports = {
  manifestHash,
  isCompat,
  defaultSignerManifest,
  createManifest,
  createVerifier
}

class StaticVerifier {
  constructor (treeHash) {
    this.treeHash = treeHash
  }

  sign () {
    return null
  }

  verify (batch, signature) {
    return b4a.equals(batch.hash(), this.treeHash)
  }
}

class CompatVerifier {
  constructor (crypto, signer, legacy) {
    validateSigner(signer)

    this.legacy = legacy
    this.crypto = crypto
    this.publicKey = signer.publicKey
  }

  sign (batch, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw BAD_ARGUMENT('No signer was passed')
    return this.crypto.sign(batch.signableCompat(this.legacy), keyPair.secretKey)
  }

  verify (batch, signature) {
    return this.crypto.verify(batch.signableCompat(this.legacy), signature, this.publicKey)
  }
}

class SingleVerifier {
  constructor (crypto, signer) {
    validateSigner(signer)

    this.crypto = crypto
    this.publicKey = signer.publicKey
    this.namespace = signer.namespace
  }

  sign (batch, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw BAD_ARGUMENT('No signer was passed')
    return this.crypto.sign(batch.signable(this.namespace), keyPair.secretKey)
  }

  verify (batch, signature) {
    return this.crypto.verify(batch.signable(this.namespace), signature, this.publicKey)
  }
}

class MultiVerifier {
  constructor (crypto, multipleSigners) {
    this.signers = multipleSigners.signers
    this.quorum = multipleSigners.quorum
    this.allowPatched = multipleSigners.allowPatched
    this.verifiers = this.signers.map(s => new SingleVerifier(crypto, s))

    if (this.verifiers.length < this.quorum || (this.quorum === 0)) throw BAD_ARGUMENT('Invalid quorum')
  }

  sign () {
    throw BAD_ARGUMENT('Multi signature must be provided')
  }

  verify (batch, signature) {
    if (!this.allowPatched) return this._verify(batch, signature)
    return this._verifyPatched(batch, signature)
  }

  _verify (batch, signature) {
    const signatures = c.decode(signatureArray, signature)

    if (signatures.length < this.quorum) return false

    let valid = 0
    const idx = this.verifiers.slice(0)

    for (const sig of signatures) {
      if (signed(batch, sig, idx)) valid++
    }

    return valid >= this.quorum
  }

  _verifyPatched (batch, signature) {
    const { proofs } = multisig.decode(signature)

    if (proofs.length < this.quorum) return false

    let valid = 0
    const idx = this.verifiers.slice(0)

    for (const proof of proofs) {
      const ref = batch.clone()

      if (proof.patch) {
        try {
          if (!ref.verifyUpgrade(proof.patch)) continue
        } catch {
          continue
        }
      }

      if (signed(ref, proof.signature, idx)) valid++
    }

    return valid >= this.quorum
  }
}

function createVerifier (manifest, { compat = false, crypto = defaultCrypto, legacy = false } = {}) {
  if (compat && manifest.signer) {
    return new CompatVerifier(crypto, manifest.signer, legacy)
  }

  if (manifest.static) {
    return new StaticVerifier(manifest.static)
  }

  if (manifest.signer) {
    return new SingleVerifier(crypto, manifest.signer)
  }

  if (manifest.multipleSigners) {
    return new MultiVerifier(crypto, manifest.multipleSigners)
  }

  throw BAD_ARGUMENT('No signer was provided')
}

function createManifest (inp) {
  if (!inp) return null

  const manifest = {
    hash: 'blake2b',
    static: null,
    signer: null,
    multipleSigners: null
  }

  if (inp.hash && inp.hash !== 'blake2b') throw BAD_ARGUMENT('Only Blake2b hashes are supported')

  if (inp.static) {
    if (!(b4a.isBuffer(inp.static) && inp.static.byteLength === 32)) throw BAD_ARGUMENT('Invalid static manifest')
    manifest.static = inp.static
    return manifest
  }

  if (inp.signer) {
    manifest.signer = parseSigner(inp.signer)
    return manifest
  }

  if (inp.multipleSigners) {
    manifest.multipleSigners = parseMultipleSigners(inp.multipleSigners)
    return manifest
  }

  throw BAD_ARGUMENT('No signer was provided')
}

function parseMultipleSigners (m) {
  if (m.signers.length < m.quorum || !(m.quorum > 0)) throw BAD_ARGUMENT('Invalid quorum')

  return {
    allowPatched: !!m.allowPatched,
    quorum: m.quorum,
    signers: m.signers.map(parseSigner)
  }
}

function parseSigner (signer) {
  validateSigner(signer)
  return {
    signature: 'ed25519',
    namespace: signer.namespace || caps.DEFAULT_NAMESPACE,
    publicKey: signer.publicKey
  }
}

function validateSigner (signer) {
  if (!signer || !signer.publicKey) throw BAD_ARGUMENT('Signer missing public key')
  if (signer.signature !== 'ed25519') throw BAD_ARGUMENT('Only Ed25519 signatures are supported')
}

function defaultSignerManifest (publicKey) {
  return {
    hash: 'blake2b',
    static: null,
    signer: {
      signature: 'ed25519',
      namespace: caps.DEFAULT_NAMESPACE,
      publicKey
    },
    multipleSigners: null
  }
}

function manifestHash (manifest) {
  const state = { start: 0, end: 32, buffer: null }
  m.manifest.preencode(state, manifest)
  state.buffer = b4a.allocUnsafe(state.end)
  c.raw.encode(state, caps.MANIFEST)
  m.manifest.encode(state, manifest)
  return defaultCrypto.hash(state.buffer)
}

function isCompat (key, manifest) {
  return !!(manifest && manifest.signer && b4a.equals(key, manifest.signer.publicKey))
}

function signed (batch, signature, idx) {
  for (let i = 0; i < idx.length; i++) {
    const indexer = idx[i]
    if (!indexer.verify(batch, signature)) continue

    const swap = idx.pop()
    if (indexer !== swap) idx[i--] = swap

    return true
  }

  return false
}
