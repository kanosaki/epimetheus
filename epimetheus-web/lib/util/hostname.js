const pattern = /\[(\d+)-(\d+)]/

export function expandHostname(exprs) {
  const results = []
  for (const exprIdx in exprs) {
    const expr = exprs[exprIdx]
    const m = pattern.exec(expr)
    if (m === null) {
      results.push(expr)
      continue
    }
    const length = m[1].length
    const begin = parseInt(m[1])
    const end = parseInt(m[2])
    for (let i = begin; i <= end; i++) {
      let first = true
      const replaced = expr.replace(m[0], () => {
        if (first) {
          first = false
          return i.toString().padStart(length, '0')
        } else {
          return m[0]
        }
      })
      expandHostname([replaced]).forEach(r => results.push(r))
    }
  }
  return results
}
