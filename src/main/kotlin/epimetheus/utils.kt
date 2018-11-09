package epimetheus

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