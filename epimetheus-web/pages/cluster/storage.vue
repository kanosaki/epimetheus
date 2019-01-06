<template>
  <div>
    <h2>Data regions</h2>
    <v-data-table
      :headers="region.headers"
      :items="data.dataRegionMetrics"
    >
      <template
        slot="items"
        slot-scope="props"
      >
        <td>{{ props.item.name }}</td>
        <td>{{ readableDataSize(props.item.pageSize * props.item.pagesFillFactor * props.item.physicalMemoryPages) }}</td>
      </template>
    </v-data-table>
  </div>
</template>

<script>
  import {clusterStorage} from "../../lib/api/cluster";
  import {readableDataSize} from "../../lib/unit";

  export default {
    data() {
      return {
        region: {
          headers: [
            {text: 'Name', value: 'name'},
            {text: 'Size', value: 'size'},
          ]
        },
        data: {
          dataRegionMetrics: [],
        },
      }
    },
    async asyncData() {
      const d = await clusterStorage()
      console.log(d.data)
      return {
        data: {
          dataRegionMetrics: d.data.dataRegionMetrics,
        }
      }
    },
    methods: {
      readableDataSize,
    },
  }
</script>
