package epimetheus.job

import org.apache.ignite.Ignite
import java.io.Serializable

interface JobRunnable : Serializable {
    fun call(ignite: Ignite): JobExitStatus
}
