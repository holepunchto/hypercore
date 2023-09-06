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

  static create (treeHash) {
    return new StaticVerifier(treeHash)
  }

  verify (signable, signature) {
    return b4a.equals(signable, this.treeHash) // todo: check batch.hash instead, need to update test
  }
}

class CompatVerifier {
  constructor (crypto, namespace, keyPair) {
    this.crypto = crypto
    this.keyPair = keyPair
  }

  static create (info, opts = {}) {
    const { keyPair, crypto, namespace } = parseSigner(info, opts)

    if (!keyPair || (keyPair.secretKey && !crypto.validateKeyPair(keyPair))) {
      throw BAD_ARGUMENT('Invalid key pair')
    }

    return new CompatVerifier(crypto, namespace, keyPair, opts)
  }

  sign (signable, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw new Error('No signer was passed')
    return this.crypto.sign(signable, keyPair.secretKey)
  }

  verify (signable, signature) {
    return this.crypto.verify(signable, signature, this.keyPair.publicKey)
  }
}

class SingleVerifier {
  constructor (crypto, namespace, keyPair) {
    this.crypto = crypto
    this.namespace = namespace
    this.keyPair = keyPair
  }

  static create (info, opts = {}) {
    const { keyPair, crypto, namespace } = parseSigner(info, opts)

    if (!keyPair || (keyPair.secretKey && !crypto.validateKeyPair(keyPair))) {
      throw BAD_ARGUMENT('Invalid key pair')
    }

    return new SingleVerifier(crypto, namespace, keyPair, opts)
  }

  sign (signable, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw new Error('No signer was passed')
    return this.crypto.sign(this._namespace(signable), keyPair.secretKey)
  }

  _namespace (signable) {
    return b4a.concat([this.namespace, signable])
  }

  verify (signable, signature) {
    return this.crypto.verify(this._namespace(signable), signature, this.keyPair.publicKey)
  }
}

class MultiVerifier {
  constructor (signers, quorum, patched) {
    this.signers = signers
    this.quorum = quorum
    this.patched = !!patched
  }

  static create (info, opts = {}) {
    const { quorum, allowPatched, signers } = info
    const verifiers = signers.map(i => SingleVerifier.create(i))

    return new MultiVerifier(verifiers, quorum, allowPatched, opts)
  }

  sign () {
    throw new Error('Multi signature must be provided.')
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

function createVerifier (manifest, opts = {}) {
  if (opts.compat && manifest.signer) {
    return CompatVerifier.create(manifest.signer, opts)
  }

  if (manifest.static) {
    return StaticVerifier.create(manifest.static)
  }

  if (manifest.signer) {
    return SingleVerifier.create(manifest.signer, opts)
  }

  if (manifest.multipleSigners) {
    return MultiVerifier.create(manifest.multipleSigners, opts)
  }

  throw BAD_ARGUMENT('Invalid manifest: no signer was provided')
}

function parseSigner (signer, opts = {}) {
  if (!signer || !signer.publicKey) throw BAD_ARGUMENT('Malformed signer.')

  const keyPair = {
    publicKey: signer.publicKey,
    secretKey: opts.keyPair ? opts.keyPair.secretKey : null
  }

  switch (signer.signature) {
    case 'ed25519':
      return {
        keyPair,
        crypto: defaultCrypto,
        namespace: signer.namespace
      }

    default:
      throw new Error('Only Ed25519 signatures are supported')
  }
}

function manifestHash (manifest) {
  const input = c.encode(m.manifest, manifest)
  const state = { start: 0, end: 32 + input.byteLength, buffer: null }
  state.buffer = b4a.allocUnsafe(state.end)
  c.raw.encode(state, caps.MANIFEST)
  c.raw.encode(state, manifest)
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
