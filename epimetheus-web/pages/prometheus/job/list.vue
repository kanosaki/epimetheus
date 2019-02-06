<template>
  <div>
    <v-toolbar
      flat
      color="white"
    >
      <v-toolbar-title>Scrape jobs</v-toolbar-title>
      <v-spacer />
      <v-btn
        color="primary"
        dark
        class="mb-2"
        :to="{path: `/prometheus/job/create`}"
      >
        New Item
      </v-btn>
    </v-toolbar>
    <v-data-table
      :headers="region.headers"
      :items="data"
      expand
      item-key="config.job_name"
    >
      <template
        slot="items"
        slot-scope="props"
      >
        <tr @click="props.expanded = !props.expanded">
          <td>{{ props.item.config.job_name }}</td>
          <td>{{ props.item.config.scrape_interval }}</td>
        </tr>
      </template>
      <template
        slot="expand"
        slot-scope="props"
      >
        <v-card flat>
          <v-card-text> Hello {{ props.item.config.job_name }}</v-card-text>
        </v-card>
      </template>
    </v-data-table>
  </div>
</template>

<script>
import { jobDiscovery } from '../../../lib/api/job'

export default {
  filters: {},
  data() {
    return {
      creatingItem: {},
      region: {
        headers: [
          { text: 'Name', value: 'job_name' },
          { text: 'Scrape Interval', value: 'scrape_interval' },
        ],
      },
      data: {},
    }
  },
  async asyncData() {
    const d = await jobDiscovery()
    return {
      data: d.data,
    }
  },
  methods: {
    close() {},
    save() {},
  },
}
</script>
