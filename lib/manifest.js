const defaultCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const c = require('compact-encoding')
const { BAD_ARGUMENT } = require('hypercore-errors')

const { multisignature } = require('./messages')
const signatureArray = c.array(c.fixed64)

const DEFAULT_ENTROPY = b4a.alloc(32)

module.exports = createAuth

function createAuth (manifest, opts) {
  const namespace = manifest.namespace

  switch (manifest.type) {
    case 'STATIC':
      return staticSigner({ treeHash: manifest.static.treeHash }, namespace)

    case 'SIGNER':
      return singleSigner(parseSigner(manifest.signer, opts), namespace, opts)

    case 'MULTI_SIGNERS': {
      const indexers = []
      for (const signer of manifest.multiSigners.signers) {
        indexers.push(singleSigner(parseSigner(signer), namespace))
      }

      const { quorum, allowPatched } = manifest.multiSigners.quorum
      const alg = allowPatched ? verifyPatched : verifyUnpatched

      return alg(indexers, quorum, opts)
    }
  }
}

function parseSigner (signer, opts = {}) {
  if (!signer || !signer.publicKey) throw new Error('Malformed signer.')

  const keyPair = {
    publicKey: signer.publicKey,
    secretKey: opts.keyPair ? opts.keyPair.secretKey : null
  }

  switch (signer.signature) {
    case 'ED_25519':
      return {
        keyPair,
        crypto: defaultCrypto,
        entropy: signer.entropy
      }

    default:
      throw new Error('Only Ed25519 signatures are supported')
  }
}

function singleSigner (signer, ns, opts = {}) {
  const crypto = signer.crypto
  const entropy = signer.entropy || DEFAULT_ENTROPY

  const keyPair = signer.keyPair
  if (!keyPair || (keyPair.secretKey && !crypto.validateKeyPair(keyPair))) {
    throw BAD_ARGUMENT('Invalid key pair')
  }

  const sign = opts.sign
    ? opts.sign
    : keyPair.secretKey
      ? (signable) => crypto.sign(namespaced(signable, ns, entropy), keyPair.secretKey)
      : undefined

  return {
    sign,
    verify (signable, signature) {
      return crypto.verify(namespaced(signable, ns, entropy), signature, keyPair.publicKey)
    }
  }
}

function staticSigner ({ treeHash }, opts = {}) {
  if (!treeHash) throw BAD_ARGUMENT('Invalid tree hash')

  return {
    verify (signable) {
      return b4a.equals(signable, treeHash)
    }
  }
}

function verifyUnpatched (indexers, threshold, opts = {}) {
  if (typeof threshold === 'object') return verifyUnpatched(indexers, null, opts)
  if (!threshold) threshold = defaultQuorum(indexers.length)

  const allowPatched = !!opts.allowPatched

  return {
    sign: opts.sign,
    verify (signable, signature) {
      const signatures = c.decode(signatureArray, signature)

      if (signatures.length < threshold) return false

      let valid = 0
      const idx = indexers.slice(0)

      for (const sig of signatures) {
        if (signed(signable, sig, idx)) valid++
      }

      return valid >= threshold
    }
  }
}

function verifyPatched (indexers, threshold, opts = {}) {
  if (typeof threshold === 'object') return verifyPatched(indexers, null, opts)
  if (!threshold) threshold = defaultQuorum(indexers.length)

  return {
    sign: opts.sign,
    verify (_, signature, batch) {
      const { proofs, nodes } = c.decode(multisignature, signature)

      if (proofs.length < threshold) return false

      let valid = 0
      const idx = indexers.slice(0)

      for (const proof of proofs) {
        const ref = batch.clone()

        if (proof.patch) {
          const patch = unpack(proof.patch, nodes)

          try {
            if (!ref.verifyUpgrade(patch)) continue
          } catch {
            continue
          }
        }

        if (signed(ref.signable(), proof.signature, idx)) valid++
      }

      return valid >= threshold
    }
  }
}

function namespaced (signable, namespace, entropy) {
  if (!entropy && !namespace) return signable
  const arr = []

  if (namespace) arr.push(namespace)
  if (entropy) arr.push(entropy)
  arr.push(signable)

  return b4a.concat(arr)
}

// 51% majority
function defaultQuorum (n) {
  if (typeof n !== 'number') throw new Error('bad type.')
  return (n >> 1) + 1
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

function unpack (s, nodes) {
  const upgrade = {
    start: s.start,
    length: s.length,
    signature: s.signature,
    nodes: [],
    additionalNodes: []
  }

  for (const i of s.nodes) {
    upgrade.nodes.push(nodes[i])
  }

  return {
    fork: 0,
    block: null,
    seek: null,
    hash: null,
    upgrade
  }
}
