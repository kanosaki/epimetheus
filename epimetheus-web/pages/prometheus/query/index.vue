<template>
  <div>
    <h2>Query</h2>
    <v-textarea
      v-model="query"
      box
      rows="1"
      name="queryInput"
      label="Prometheus query"
      @change="onQueryUpdated()"
    />
    <v-chart
      :options="datum"
      auto-resize
    />
  </div>
</template>
<style>
  .echarts {
    width: 100%;
    height: 300px;
  }
</style>

<script>
  import {queryRange} from "../../../lib/api/prometheus/query";
  const defaultQuery = 'rate(node_cpu[1m])'
  const queryCookieKey = 'query-saved'

  const chartOptions = {
    title: {
      text: 'ECharts entry example'
    },
    tooltip: {
      trigger: 'axis',
      textStyle: {
        fontSize: 11,
      },
      confine: true,
      axisPointer: {
        type: 'cross',
      },
    },
    grid: {
      left: '80px',
      right: '20px',
      top: 20,
      bottom: 30,
    },
    yAxis: {},
    animationDuration: 200,
  }

  export default {
    data() {
      return {
        query: this.$cookies.get(queryCookieKey) || defaultQuery,
        datum: {}
      }
    },
    async asyncData({ app }) {
      const result = await queryRange(app.$cookies.get(queryCookieKey) || defaultQuery, 60 * 60)
      return {
        datum: result.renderEchartsOptions(chartOptions)
      }
    },
    methods: {
      async onQueryUpdated() {
        this.$cookies.set(queryCookieKey, this.query)
        const result = await queryRange(this.query, 60 * 60)
        this.datum = result.renderEchartsOptions(chartOptions)
      }
    }
  }
</script>
