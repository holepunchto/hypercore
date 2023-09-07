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
  createVerifier
}

class StaticVerifier {
  constructor (treeHash) {
    this.treeHash = treeHash
  }

  sign () {
    return null
  }

  verify (signable, signature) {
    return b4a.equals(signable, this.treeHash) // todo: check batch.hash instead, need to update test
  }
}

class CompatVerifier {
  constructor (crypto, signer) {
    validateSigner(signer)

    this.crypto = crypto
    this.publicKey = signer.publicKey
  }

  sign (signable, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw BAD_ARGUMENT('No signer was passed')
    return this.crypto.sign(signable, keyPair.secretKey)
  }

  verify (signable, signature) {
    return this.crypto.verify(signable, signature, this.publicKey)
  }
}

class SingleVerifier {
  constructor (crypto, signer) {
    validateSigner(signer)

    this.crypto = crypto
    this.publicKey = signer.publicKey
    this.namespace = signer.namespace
  }

  sign (signable, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw BAD_ARGUMENT('No signer was passed')
    return this.crypto.sign(this._namespace(signable), keyPair.secretKey)
  }

  verify (signable, signature) {
    return this.crypto.verify(this._namespace(signable), signature, this.publicKey)
  }

  _namespace (signable) {
    return b4a.concat([this.namespace, signable])
  }
}

class MultiVerifier {
  constructor (crypto, multipleSigners) {
    this.signers = multipleSigners.signers
    this.quorum = multipleSigners.quorum
    this.patched = !!multipleSigners.patched
    this.verifiers = this.signers.map(s => new StaticVerifier(crypto, s))
  }

  sign () {
    throw new BAD_ARGUMENT('Multi signature must be provided')
  }

  verify (signable, signature, batch) {
    if (!this.patched) return this._verify(signable, signature)
    return this._verifyPatched(signable, signature, batch)
  }

  _verify (signable, signature) {
    const signatures = c.decode(signatureArray, signature)

    if (signatures.length < this.quorum) return false

    let valid = 0
    const idx = this.signers.slice(0)

    for (const sig of signatures) {
      if (signed(signable, sig, idx)) valid++
    }

    return valid >= this.quorum
  }

  _verifyPatched (_, signature, batch) {
    const { proofs } = multisig.decode(signature)

    if (proofs.length < this.quorum) return false

    let valid = 0
    const idx = this.signers.slice(0)

    for (const proof of proofs) {
      const ref = batch.clone()

      if (proof.patch) {
        try {
          if (!ref.verifyUpgrade(proof.patch)) continue
        } catch {
          continue
        }
      }

      if (signed(ref.signable(), proof.signature, idx)) valid++
    }

    return valid >= this.quorum
  }
}

function createVerifier (manifest, { compat = false, crypto = defaultCrypto } = {}) {
  if (compat && manifest.signer) {
    return new CompatVerifier(crypto, manifest.signer)
  }

  if (manifest.static) {
    return new StaticVerifier(crypto, manifest.static)
  }

  if (manifest.signer) {
    return new SingleVerifier(crypto, manifest.signer)
  }

  if (manifest.multipleSigners) {
    return new MultiVerifier(crypto, manifest.multipleSigners)
  }

  throw BAD_ARGUMENT('Invalid manifest: no signer was provided')
}

function validateSigner (signer) {
  if (!signer || !signer.publicKey) throw BAD_ARGUMENT('Malformed signer')
  if (signer.signature !== 'ed25519') throw BAD_ARGUMENT('Only Ed25519 signatures are supported')
}

function manifestHash (manifest) {
  const state = { start: 0, end: 32, buffer: null }
  m.manifest.preencode(state, manifest)
  state.buffer = b4a.allocUnsafe(state.end)
  c.raw.encode(state, caps.MANIFEST)
  m.manifest.encode(state, manifest)
  return defaultCrypto.hash(state.buffer)
}

function signed (signable, signature, idx) {
  for (let i = 0; i < idx.length; i++) {
    const indexer = idx[i]
    if (!indexer.verify(signable, signature)) continue

    const swap = idx.pop()
    if (indexer !== swap) idx[i--] = swap

    return true
  }

  return false
}
