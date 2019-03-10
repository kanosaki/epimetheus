package epimetheus.job

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CachePeekMode
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.services.ServiceConfiguration
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("slow")
class JobTest {
    data class DummyJobKey(val name: String) : JobKey
    data class CallHistoryKey(val name: String, val timestamp: Long)

    private lateinit var ignite: Ignite
    private lateinit var callHistory: IgniteCache<CallHistoryKey, Int>
    private val pollInterval = 100L
    private val serviceName = "test_job_service"

    @BeforeAll
    fun setUp() {
        ignite = Ignition.getOrStart(IgniteConfiguration())
        ignite.services().deploy(ServiceConfiguration().apply {
            name = serviceName
            service = JobService(pollInterval)
            maxPerNodeCount = 1
        })
        callHistory = ignite.getOrCreateCache("call_history")
    }

    @AfterAll
    fun tearDown() {
        ignite.services().cancel(serviceName)
        callHistory.destroy()
        JobGateway(ignite).clear()
    }

    @AfterEach
    fun clearCallHistory() {
        callHistory.clear()
    }

    @Test
    fun testShotCountAndInterval() {
        val gateway = JobGateway(ignite)
        val repeatCount = 10
        val interval = 100L
        val overhead = 20L
        gateway.scheduleInterval(
                DummyJobKey("foo"),
                object : JobRunnable {
                    override fun call(ignite: Ignite): JobExitStatus {
                        callHistory.put(CallHistoryKey("singleShot", System.currentTimeMillis()), 1)
                        if (callHistory.size(CachePeekMode.PRIMARY) >= repeatCount) {
                            return JobExitStatus.Terminate
                        }
                        return JobExitStatus.Done
                    }
                },
                Duration.ofMillis(interval))
        Thread.sleep(pollInterval * 2 + repeatCount * (interval + overhead)) // 20 for margin
        val histories = callHistory.toList().sortedBy { it.key.timestamp }
        val intervals = mutableListOf<Long>()
        var prev = 0L
        for (h in histories) {
            if (prev == 0L) {
                prev = h.key.timestamp
                continue
            }
            intervals.add(h.key.timestamp - prev)
            prev = h.key.timestamp
        }
        assertEquals(intervals.size, repeatCount - 1)
        assertAll("executed periodically",
                intervals.map { { assertTrue(Math.abs(it - interval) < overhead) } }
        )
    }
}
