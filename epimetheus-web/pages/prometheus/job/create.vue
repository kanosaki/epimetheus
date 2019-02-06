<template>
  <v-container fluid>
    <v-layout row>
      <v-flex grow>
        <h1>Add job</h1>
      </v-flex>
      <v-flex shrink>
        <v-btn
          label="Federation"
          :value="federation"
        >
          Federation
        </v-btn>
      </v-flex>
    </v-layout>
    <edit-job v-model="item" create="true" />
    <v-layout row>
      <v-spacer />
      <v-flex shrink>
        <v-btn
          label="OK"
          @click="submit"
          primary
        >
          OK
        </v-btn>
      </v-flex>
    </v-layout>
  </v-container>
</template>

<script lang="ts">
import EditJob from '../../../components/prometheus/job/EditJob'

export default {
  components: {
    EditJob,
  },
  methods: {
    submit() {
      console.log(this.item)
    },
  },
  data() {
    return {
      federation: false,
      item: {
        name: '',
        path: '/metrics',
        scheme: 'http',
        honorLabels: false,
        discoveries: [{ type: 'static', input: '' }],
      },
    }
  },
}
</script>
