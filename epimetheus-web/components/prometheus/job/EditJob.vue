<template>
  <div>
    <v-layout wrap row>
      <v-flex md6 px-2>
        <v-text-field
          v-model="value.name"
          :disabled="!create"
          label="Job name"
          required
        />
      </v-flex>
      <v-flex md6 px-2>
        <v-text-field
          v-model="value.path"
          label="Path"
          required
        />
      </v-flex>
      <v-flex md6 px-2>
        <v-select
          v-model="value.scheme"
          label="Scheme"
          :items="schemeItems"
        />
      </v-flex>
      <v-flex md6 px-2>
        <v-checkbox
          v-model="value.honorLabels"
          label="Honor labels"
        />
      </v-flex>
    </v-layout>
    <v-layout column>
      <v-flex v-for="(item, index) in value.discoveries" :key="index">
        <div v-if="item.type === 'static'">
          <h2>Static targets</h2>
          <edit-static-config :item="item" />
        </div>
      </v-flex>
    </v-layout>
  </div>
</template>

<script lang="ts">
import EditStaticConfig from './discovery/EditStaticConfig.vue'

export default {
  components: {
    EditStaticConfig,
  },
  props: ['value', 'create'],
  data() {
    return {
      schemeItems: ['http', 'https'],
    }
  },
}
</script>
