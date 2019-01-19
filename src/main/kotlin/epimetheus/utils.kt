package epimetheus

import org.apache.ignite.IgniteTransactions
import org.apache.ignite.transactions.Transaction
import java.time.Duration

object DurationUtil {
    fun Duration.toPromString(): String {
        val sb = StringBuilder()
        val hours = this.toDays() * 24 + this.toHours()
        if (hours > 0) {
            sb.append(hours)
            sb.append("h")
        }
        val minutes = this.toMinutes()
        if (minutes in 1..59) {
            sb.append(minutes)
            sb.append("m")
        }
        val seconds = this.toMillis() / 1000
        if (seconds in 1..59) {
            sb.append(seconds)
            sb.append("s")
        }
        val msecs = this.toMillis() % 1000
        if (msecs in 1..999) {
            sb.append(msecs)
            sb.append("ms")
        }
        return sb.toString()
    }
}

/**
 * Wrap code block with Ignite transaction,
 */
inline fun transaction(igniteTx: IgniteTransactions, fn: (Transaction) -> Boolean) {
    val outer = igniteTx.tx()
    val tx = outer ?: igniteTx.txStart()
    try {
        val succeed = fn(tx)
        if (outer == null && succeed) {
            tx.commit()
        }
    } finally {
        // if fn return false, tx will be rolled back automatically here
        if (outer == null) {
            tx.close()
        }
    }
}