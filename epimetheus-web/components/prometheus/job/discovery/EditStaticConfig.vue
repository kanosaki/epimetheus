<template>
  <v-layout wrap>
    <v-flex md6>
      <v-textarea
        v-model="item.input"
        label="Target hosts"
        box
        px-2
      />
    </v-flex>
    <v-flex md6 px-2>
      <v-textarea
        v-model="expandedHosts"
        label="Hosts"
        disabled
      />
    </v-flex>
  </v-layout>
</template>

<script>
import { expandHostname } from '../../../../lib/util/hostname'

export default {
  props: ['item'],
  computed: {
    expandedHosts() {
      const lines = this.item.input.split(/\r?\n/g)
      return expandHostname(lines).join('\n')
    },
  },
}
</script>
