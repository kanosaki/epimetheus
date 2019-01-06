import axios from 'axios'

export function stepSeconds(durationSeconds, expectedTimePoints) {
  return Math.floor(durationSeconds / expectedTimePoints)
}

export async function queryRange(query, durationSeconds, latestTimestamp, stepSecs) {
  if (!query) {
    throw 'query required'
  }
  if (!durationSeconds) {
    throw 'duration seconds required'
  }
  if (!latestTimestamp) {
    latestTimestamp = new Date().getTime() / 1000.0
  }
  if (!stepSecs) {
    stepSecs = stepSeconds(durationSeconds, 100)
  }
  const resp = await axios.get(`http://localhost:9090/api/v1/query_range`, {
    params: {
      query: query,
      start: latestTimestamp - durationSeconds,
      end: latestTimestamp,
      step: stepSecs,
    }
  })
  return new QueryRangeResult(resp.data)
}

export class QueryRangeResult {
  constructor(resp) {
    this.data = resp.data
    this.status = resp.status // success or error
    this.error = resp.error
    this.errorType = resp.errorType
    this.resultType = resp.resultType // 'matrix' or 'vector', will be 'matrix' if 2 or more timestamps
  }

  static metricStringify(dic) {
    const name = dic['__name__'] || ''
    const attrs = []
    for (let prop in dic) {
      if (!dic.hasOwnProperty(prop)) continue;
      if (prop === '__name__') continue;
      attrs.push(`${prop}="${dic[prop]}"`)
    }
    if (name === '') {
      return `{${attrs.join(',')}}`
    } else {
      if (attrs.length === 0) {
        return name
      } else {
        return `${name}{${attrs.join(',')}}`
      }
    }
  }

  renderEchartsOptions(base) {
    const series = []
    let timestamps = null
    this.data.result.forEach(sel => {
      series.push({
        name: QueryRangeResult.metricStringify(sel.metric),
        symbol: 'none',
        type: 'line',
        data: sel.values.map(i => [i[0], parseFloat(i[1])]),
      })
    })

    return Object.assign({}, base, {
      xAxis: {
        data: timestamps,
        type: 'time',
      },
      series: series,
    })
  }
}
