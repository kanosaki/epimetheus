<template>
  <div>
    <v-toolbar
      flat
      color="white"
    >
      <v-toolbar-title>Scrape jobs</v-toolbar-title>
      <v-spacer/>
      <v-dialog
        max-width="500px"
      >
        <v-btn
          slot="activator"
          color="primary"
          dark
          class="mb-2"
        >
          New Item
        </v-btn>
        <v-card>
          <v-card-title>
            <span class="headline">Create new job</span>
          </v-card-title>

          <v-card-text>
            <v-container grid-list-md>
              <v-layout wrap>
                <v-flex
                  xs12
                  sm6
                  md4
                >
                  <v-text-field
                    v-model="creatingItem.name"
                    label="Dessert name"
                  />
                </v-flex>
              </v-layout>
            </v-container>
          </v-card-text>

          <v-card-actions>
            <v-spacer />
            <v-btn
              color="blue darken-1"
              flat
              @click="close"
            >
              Cancel
            </v-btn>
            <v-btn
              color="blue darken-1"
              flat
              @click="save"
            >
              Save
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-dialog>
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
          <v-card-text>
            Hello {{ props.item.config.job_name }}
          </v-card-text>
        </v-card>
      </template>
    </v-data-table>
  </div>
</template>

<script>
  import {jobDiscovery} from "../../../lib/api/job";

  export default {
    filters: {},
    data() {
      return {
        creatingItem: {},
        region: {
          headers: [
            {text: 'Name', value: 'job_name'},
            {text: 'Scrape Interval', value: 'scrape_interval'},
          ]
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
      close() {

      },
      save() {

      }
    },
  }
</script>
