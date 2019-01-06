<template>
  <v-app>
    <v-navigation-drawer
      stateless
      value="true"
      clipped
      app
    >
      <v-list dense>
        <v-list-tile :to="{path: `/`}">
          <v-list-tile-action>
            <v-icon>home</v-icon>
          </v-list-tile-action>
          <v-list-tile-title>Home</v-list-tile-title>
        </v-list-tile>

        <v-list-group
          prepend-icon="timeline"
          value="true"
        >
          <v-list-tile
            slot="activator"
            :to="{path: `/prometheus`}"
          >
            <v-list-tile-title>Prometheus</v-list-tile-title>
          </v-list-tile>

          <v-list-group
            no-action
            sub-group
          >
            <v-list-tile
              slot="activator"
              :to="{path: `/prometheus/query`}"
            >
              <v-list-tile-title>Query</v-list-tile-title>
            </v-list-tile>
            <v-list-tile :to="{path: `/prometheus/query/explain`}">
              <v-list-tile-title>Explain</v-list-tile-title>
            </v-list-tile>
            <v-list-tile :to="{path: `/prometheus/query/trace`}">
              <v-list-tile-title>Trace</v-list-tile-title>
            </v-list-tile>
          </v-list-group>

          <v-list-group
            no-action
            sub-group
          >
            <v-list-tile
              slot="activator"
              :to="{path: `/prometheus/job`}"
            >
              <v-list-tile-title>Jobs</v-list-tile-title>
            </v-list-tile>

            <v-list-tile :to="{path: `/prometheus/job/status`}">
              <v-list-tile-title>Status</v-list-tile-title>
            </v-list-tile>
            <v-list-tile :to="{path: `/prometheus/job/list`}">
              <v-list-tile-title>Edit</v-list-tile-title>
            </v-list-tile>
          </v-list-group>

          <v-list-group
            sub-group
            no-action
          >
            <v-list-tile slot="activator">
              <v-list-tile-title>Rules</v-list-tile-title>
            </v-list-tile>

            <v-list-tile :to="{path: `/prometheus/rule/recording`}">
              <v-list-tile-title>Recording</v-list-tile-title>
              <v-list-tile-action>
                <v-icon>crd</v-icon>
              </v-list-tile-action>
            </v-list-tile>

            <v-list-tile :to="{path: `/prometheus/rule/alert`}">
              <v-list-tile-title>Alert</v-list-tile-title>
              <v-list-tile-action>
                <v-icon>crd</v-icon>
              </v-list-tile-action>
            </v-list-tile>
          </v-list-group>
        </v-list-group>
        <v-list-group
          prepend-icon="dns"
          value="true"
        >
          <v-list-tile
            slot="activator"
            :to="{path: `/cluster`}"
          >
            <v-list-tile-title>Cluster</v-list-tile-title>
          </v-list-tile>

          <v-list-group
            no-action
            sub-group
          >
            <v-list-tile
              slot="activator"
              :to="{path: `/cluster/nodes`}"
            >
              <v-list-tile-title>Nodes</v-list-tile-title>
            </v-list-tile>
          </v-list-group>

          <v-list-group
            sub-group
            no-action
          >
            <v-list-tile
              slot="activator"
              :to="{path: `/cluster/storage`}"
            >
              <v-list-tile-title>Storage</v-list-tile-title>
            </v-list-tile>
          </v-list-group>
        </v-list-group>
      </v-list>
    </v-navigation-drawer>
    <v-toolbar
      dense
      app
      clipped-left
    >
      <v-toolbar-side-icon/>
      <v-toolbar-title>Epimetheus</v-toolbar-title>
      <v-spacer/>
      <v-toolbar-items class="hidden-sm-and-down">
        <v-btn flat>ALERTS: N</v-btn>
        <v-btn flat>STATUS: GREEN</v-btn>
      </v-toolbar-items>
    </v-toolbar>
    <v-content>
      <v-container>
        <nuxt/>
      </v-container>
    </v-content>
  </v-app>
</template>

<script>
  export default {
    data() {
      return {
        clipped: false,
        drawer: true,
        fixed: false,
        items: [
          {icon: 'apps', title: 'Welcome', to: '/'},
          {icon: 'bubble_chart', title: 'Inspire', to: '/inspire'}
        ],
        miniVariant: false,
        right: true,
        rightDrawer: false,
        title: 'Vuetify.js'
      }
    }
  }
</script>
