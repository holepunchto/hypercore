const TICKS = 16

module.exports = class HotswapQueue {
  constructor () {
    this.priorities = [[], [], []]
  }

  * pick (peer) {
    for (let i = 0; i < this.priorities.length; i++) {
      // try first one more than second one etc etc
      let ticks = (this.priorities.length - i) * TICKS
      const queue = this.priorities[i]

      for (let j = 0; j < queue.length; j++) {
        const r = j + Math.floor(Math.random() * queue.length - j)
        const a = queue[j]
        const b = queue[r]

        if (r !== j) {
          queue[(b.hotswap.index = j)] = b
          queue[(a.hotswap.index = r)] = a
        }

        if (hasInflight(b, peer)) continue

        yield b

        if (--ticks <= 0) break
      }
    }
  }

  add (block) {
    if (block.hotswap !== null) this.remove(block)
    if (block.inflight.length === 0 || block.inflight.length >= 3) return

    // TODO: also use other stuff to determine queue prio
    const queue = this.priorities[block.inflight.length - 1]

    const index = queue.push(block) - 1
    block.hotswap = { ref: this, queue, index }
  }

  remove (block) {
    const hotswap = block.hotswap
    if (hotswap === null) return

    block.hotswap = null
    const head = hotswap.queue.pop()
    if (head === block) return
    hotswap.queue[(head.hotswap.index = hotswap.index)] = head
  }
}

function hasInflight (block, peer) {
  for (let j = 0; j < block.inflight.length; j++) {
    if (block.inflight[j].peer === peer) return true
  }
  return false
}
