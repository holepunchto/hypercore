/** !
 * Fast CRC32 in JavaScript
 * 101arrowz (https://github.com/101arrowz)
 * License: MIT
 */

// Lightly-modified from https://gist.github.com/101arrowz/e58695f7ccfdf74f60ba22018093edea

const crct = new Int32Array(4096)
for (let i = 0; i < 256; ++i) {
  let c = i; let k = 9
  while (--k) c = ((c & 1) && -306674912) ^ (c >>> 1)
  crct[i] = c
}
for (let i = 0; i < 256; ++i) {
  let lv = crct[i]
  for (let j = 256; j < 4096; j += 256) lv = crct[i | j] = (lv >>> 8) ^ crct[lv & 255]
}

const crcts = []

for (let i = 0; i < 16;) crcts[i] = crct.subarray(i << 8, ++i << 8)

const [
  t1, t2, t3, t4, t5, t6, t7, t8,
  t9, t10, t11, t12, t13, t14, t15, t16
] = crcts

module.exports = function crc32 (d) {
  let c = 0
  let i = 0
  const max = d.length - 16
  for (; i < max;) {
    c =
        t16[d[i++] ^ (c & 255)] ^
        t15[d[i++] ^ ((c >> 8) & 255)] ^
        t14[d[i++] ^ ((c >> 16) & 255)] ^
        t13[d[i++] ^ (c >>> 24)] ^
        t12[d[i++]] ^
        t11[d[i++]] ^
        t10[d[i++]] ^
        t9[d[i++]] ^
        t8[d[i++]] ^
        t7[d[i++]] ^
        t6[d[i++]] ^
        t5[d[i++]] ^
        t4[d[i++]] ^
        t3[d[i++]] ^
        t2[d[i++]] ^
        t1[d[i++]]
  }
  for (; i < d.length; ++i) c = t1[(c & 255) ^ d[i]] ^ (c >>> 8)
  return ~c
}
