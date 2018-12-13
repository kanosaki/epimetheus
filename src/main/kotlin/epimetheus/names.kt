package epimetheus

object CacheName {
    const val CONFIG = "config"

    object Prometheus {
        /**
         * Scrape target hosts and its configurations. (Infrequently updated, persistent)
         */
        const val SCRAPE_TARGETS = "scrapeTargets"

        /**
         * Stores scrape result for each targets.
         */
        const val SCRAPE_STATUSES = "scrapeStatuses"

        /**
         * Stores recently fetched samples, focused on throughput rather than capacity.
         */
        const val FRESH_SAMPLES = "eden"

        const val METRIC_META = "metric_meta"
    }
}

object ConfigKey {
    const val API_SERVER = "api-server"
}

object ServiceName {
    object Prometheus {
        const val SCRAPE_SERVICE = "scrape"
        const val API_SERVER = "api"
    }
}