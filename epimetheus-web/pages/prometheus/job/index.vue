<template>
  <div>
    <h2>Scrape status</h2>
    <v-data-table :headers="region.headers" :items="data">
      <template slot="items" slot-scope="props">
        <td>{{ props.item.key.jobName }}</td>
        <td>{{ props.item.key.target }}</td>
        <td>{{ props.item.status.lastResult | renderLastStatus }}</td>
        <td>{{ props.item.status.lastTimestamp | renderPrevTime }}</td>
        <td>{{ props.item.status.nextExec | renderNextExec }}</td>
      </template>
    </v-data-table>
  </div>
</template>

<script>
import { jobStatus } from '../../../lib/api/job'
import moment from 'moment'

export default {
  filters: {
    // status: epimetheus.prometheus.scrape.ScrapeStatus
    renderLastStatus(status) {
      if (status.status === 'ok') {
        return `OK: ${status.latencyNs / 1000 / 1000}ms`
      } else if (status.status === 'error') {
        return `Error: ${status.reason}`
      } else {
        return `Unknown(${JSON.stringify(status)})`
      }
    },
    // ts: epoch mills
    renderPrevTime(ts) {
      const now = new Date().getTime()
      return `${moment.duration(now - ts, 'ms').asSeconds()}s ago`
    },
    // ts: epoch mills
    renderNextExec(ts) {
      const now = new Date().getTime()
      return `${moment(ts).format('HH:mm:ss')} (${moment
        .duration(ts - now)
        .asSeconds()}s)`
    },
  },
  data() {
    return {
      region: {
        headers: [
          { text: 'Name', value: 'name' },
          { text: 'Target', value: 'target' },
          { text: 'Last Status', value: 'lastResult' },
          { text: 'Last Exec', value: 'lastTimestamp' },
          { text: 'Next Exec', value: 'nextExec' },
        ],
      },
      data: {
        dataRegionMetrics: [],
      },
    }
  },
  async asyncData() {
    const d = await jobStatus()
    return {
      data: d.data,
    }
  },
  methods: {},
}
</script>
