package epimetheus

import epimetheus.prometheus.configfile.PrometheusGlobalConfig
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CacheConfiguration
import java.time.ZoneId


class ClusterConfig(val ignite: Ignite) {
    private val cc = CacheConfiguration<String, Any>().apply {
        name = CacheName.CONFIG
        backups = 1
    }
    private val cache = ignite.getOrCreateCache(cc)

    init {
        // set default value to prevent cache returns null
        if (cache.get(Key.TIMEZONE.key) == null) {
            cache.put(Key.TIMEZONE.key, ZoneId.of("Z")) // Use utc as default
        }
    }

    var prometheusGlobal: PrometheusGlobalConfig
        get() = cache.get(Key.PROMETHEUS_GLOBAL.key) as PrometheusGlobalConfig
        set(value) = cache.put(Key.PROMETHEUS_GLOBAL.key, value)

    var timeZone: ZoneId
        get() = cache.get(Key.TIMEZONE.key) as ZoneId
        set(value) = cache.put(Key.TIMEZONE.key, value)


    enum class Key(val key: String) {
        PROMETHEUS_GLOBAL("prometheus-global"),
        TIMEZONE("timezone"),
    }
}
