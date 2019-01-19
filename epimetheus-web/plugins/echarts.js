import Vue from 'vue'
import ECharts from '../node_modules/vue-echarts/components/ECharts'

// import 'echarts/lib/chart/bar'
import 'echarts/lib/chart/line'
// import 'echarts/lib/chart/pie'
// import 'echarts/lib/chart/scatter'
import 'echarts/lib/component/tooltip'
import 'echarts/lib/component/axisPointer'

Vue.component('v-chart', ECharts)
