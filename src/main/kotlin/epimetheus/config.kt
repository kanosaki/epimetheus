package epimetheus

import epimetheus.prometheus.configfile.PrometheusGlobalConfig
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CacheConfiguration


class ClusterConfig(val ignite: Ignite) {
    private val cc = CacheConfiguration<String, Any>().apply {
        name = CacheName.CONFIG
        backups = 1
    }
    private val cache = ignite.getOrCreateCache(cc)

    var prometheusGlobal: PrometheusGlobalConfig
        get() = cache.get(Key.PROMETHEUS_GLOBAL.key) as PrometheusGlobalConfig
        set(value) = cache.put(Key.PROMETHEUS_GLOBAL.key, value)


    enum class Key(val key: String) {
        PROMETHEUS_GLOBAL("prometheus-global")
    }
}