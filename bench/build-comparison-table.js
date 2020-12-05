const globby = require('globby')
const fs = require('fs')
const fsPromises = fs.promises
const { format } = require('d3-format')

// Generate a markdown table to compare benchmarks between branches. First write
// the benchmark results from each branch to a file:
//
// ```
// $ git checkout master
// $ npm run bench > bench.master.log
// $ git checkout my-branch
// $ npm run bench > bench.my-branch.log
// ```
//
// Then generate the table:
//
// ```
// node bench/build-comparison-table.js > bench-comparison-table.md
// ```

;(async () => {
  const paths = await globby('branch.*.log')

  const benchmarks = await Promise.all(paths.map(async (filepath) => {
    const data = await fsPromises.readFile(filepath, 'utf8')
    const benchmark = parseBenchmarks(data)
    const [, branch, round] = filepath.match(/^branch\.(.*)\.round-(\d+)/)
    return {
      branch: branch === 'master' ? 'before' : 'after',
      round: Number.parseInt(round),
      results: benchmark
    }
  }))
  const agg = {}

  for (const { branch, results } of benchmarks) {
    for (const { name, bytesPerSecond, blocksPerSecond } of results) {
      agg[name] = agg[name] || {}
      const result = agg[name][branch] = agg[name][branch] || []
      result.bytesPerSecond = (result.bytesPerSecond || []).concat(bytesPerSecond)
      result.blocksPerSecond = (result.blocksPerSecond || []).concat(blocksPerSecond)
    }
  }

  const rows = [
    ['name', 'before', 'after', 'diff'],
    ['----', '------', '-----', '----']
  ]
  for (const [name, { before, after }] of Object.entries(agg)) {
    const bytesBeforeAvg = avg(before.bytesPerSecond)
    const bytesAfterAvg = avg(after.bytesPerSecond)
    const blocksBeforeAvg = avg(before.blocksPerSecond)
    const blocksAfterAvg = avg(after.blocksPerSecond)

    const bytesDiff = (bytesAfterAvg - bytesBeforeAvg) / bytesBeforeAvg
    const blocksDiff = (blocksAfterAvg - blocksBeforeAvg) / blocksBeforeAvg
    rows.push([
      name,
      `${format(',')(bytesBeforeAvg)} bytes/s<br/>${format(',')(blocksBeforeAvg)} blocks/s`,
      `${format(',')(bytesAfterAvg)} bytes/s<br/>${format(',')(blocksAfterAvg)} blocks/s`,
      `${format('+.2%')(bytesDiff)} bytes/s<br/>${format('+.2%')(blocksDiff)} blocks/s`
    ])
  }

  const tableString = rows.map(row => '| ' + row.join(' | ') + ' |').join('\n')
  process.stdout.write(tableString)
})()

function avg (arr) {
  return arr.reduce((acc, val) => acc + val) / arr.length
}

function parseBenchmarks (string) {
  return string.split('\n\n')
    .filter(chunk => chunk.startsWith('> node '))
    .map(chunk => {
      const [cmd, bytesResult, blocksResult] = chunk.split('\n')
      return {
        name: cmd.replace(/^> node /, '').replace(/\.js$/, ''),
        bytesPerSecond: Number.parseInt(bytesResult.split(' ')[0]),
        blocksPerSecond: Number.parseInt(blocksResult.split(' ')[0])
      }
    })
}
