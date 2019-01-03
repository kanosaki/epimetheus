package epimetheus.prometheus

import java.time.Duration

interface Discovery {
    val updateInterval: Duration
    fun update()
}

