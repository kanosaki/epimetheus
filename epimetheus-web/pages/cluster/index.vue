<template>
  <div>
    <h1>Cluster</h1>
    <h2>Info</h2>
    <ul>
      <li>Total Nodes: {{ data.metrics.totalNodes }}</li>
      <li>Total CPUs: {{ data.metrics.totalCpus }}</li>
      <li>Average CPU Load: {{ data.metrics.averageCpuLoad }}</li>
      <li>
        Heap(used/max): {{ readableDataSize(data.metrics.heapMemoryUsed) }}/{{
          readableDataSize(data.metrics.heapMemoryMaximum)
        }}
      </li>
      <li>
        OffHeap(used/commited/max):
        {{ readableDataSize(data.metrics.nonHeapMemoryUsed) }}/{{
          readableDataSize(data.metrics.nonHeapMemoryCommitted)
        }}/{{ readableDataSize(data.metrics.nonHeapMemoryMaximum) }}
      </li>
    </ul>
    <h2>Nodes</h2>
    <v-data-table :headers="nodesHeaders" :items="data.nodes" hide-actions>
      <template slot="items" slot-scope="props">
        <td>{{ props.item.hostnames.join(',') }}</td>
        <td>{{ props.item.version }}</td>
        <td>{{ props.item.id }}</td>
      </template>
    </v-data-table>
  </div>
</template>

<script>
import { clusterInfo } from '../../lib/api/cluster'
import { readableDataSize } from '../../lib/unit'

export default {
  data() {
    return {
      nodesHeaders: [
        { text: 'Hostnames', value: 'hostnames' },
        { text: 'Version', value: 'version' },
        { text: 'ID', value: 'id' },
      ],
      data: {
        metrics: {},
      },
    }
  },
  async asyncData() {
    const info = await clusterInfo()
    console.dir(info)
    return {
      data: info.data,
    }
  },
  methods: {
    readableDataSize,
  },
}
</script>
