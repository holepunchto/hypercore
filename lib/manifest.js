const defaultCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const c = require('compact-encoding')
const { BAD_ARGUMENT } = require('hypercore-errors')

const multisig = require('./multisig')

const signatureArray = c.array(c.fixed64)

const DEFAULT_NAMESPACE = b4a.alloc(32, 0)

class StaticVerifier {
  constructor (treeHash) {
    this.treeHash = treeHash
  }

  static create (treeHash) {
    return new StaticVerifier(treeHash)
  }

  verify (signable, signature, batch) {
    return b4a.equals(signable, this.treeHash) // todo: check batch.hash instead, need to update test
  }
}

class CompatVerifier {
  constructor (crypto, namespace, keyPair, opts = {}) {
    this.crypto = crypto
    this.keyPair = keyPair

    this.sign = opts.sign ? opts.sign : keyPair.secretKey && this._sign.bind(this)
  }

  static create (info, opts = {}) {
    const { keyPair, crypto, namespace } = parseSigner(info, opts)

    if (!keyPair || (keyPair.secretKey && !crypto.validateKeyPair(keyPair))) {
      throw BAD_ARGUMENT('Invalid key pair')
    }

    return new CompatVerifier(crypto, namespace, keyPair, opts)
  }

  _sign (signable) {
    return this.crypto.sign(signable, this.keyPair.secretKey)
  }

  verify (signable, signature) {
    return this.crypto.verify(signable, signature, this.keyPair.publicKey)
  }
}

class SingleVerifier {
  constructor (crypto, namespace, keyPair, opts = {}) {
    this.crypto = crypto
    this.namespace = namespace || DEFAULT_NAMESPACE
    this.keyPair = keyPair

    this.sign = opts.sign ? opts.sign : keyPair.secretKey && this._sign.bind(this)
  }

  static create (info, opts = {}) {
    const { keyPair, crypto, namespace } = parseSigner(info, opts)

    if (!keyPair || (keyPair.secretKey && !crypto.validateKeyPair(keyPair))) {
      throw BAD_ARGUMENT('Invalid key pair')
    }

    return new SingleVerifier(crypto, namespace, keyPair, opts)
  }

  _sign (signable) {
    return this.crypto.sign(this._namespace(signable), this.keyPair.secretKey)
  }

  _namespace (signable) {
    if (this.compat) return signable
    return b4a.concat([this.namespace, signable])
  }

  verify (signable, signature) {
    return this.crypto.verify(this._namespace(signable), signature, this.keyPair.publicKey)
  }
}

class MultiVerifier {
  constructor (signers, quorum, patched, opts = {}) {
    this.signers = signers
    this.quorum = quorum
    this.patched = !!patched
    this.sign = opts.sign
  }

  static create (info, opts = {}) {
    const { quorum, allowPatched, signers } = info
    const verifiers = signers.map(i => SingleVerifier.create(i))

    return new MultiVerifier(verifiers, quorum, allowPatched, opts)
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

module.exports = function createAuth (manifest, opts = {}) {
  if (opts.compat) {
    console.log(opts.compat)
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

function signed (signable, signature, idx) {
  for (let i = 0; i < idx.length; i++) {
    const indexer = idx[i]
    if (!indexer.verify(signable, signature)) continue

    const swap = idx.pop()
    if (indexer !== swap) idx[i] = swap

    return true
  }

  return false
}
