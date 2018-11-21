package epimetheus.pkg.promql

object Utils {
    fun isValidLabelName(cs: String): Boolean {
        // Refer: PromQLLexer .dropMetricName()
        // NAME
        // : [a-zA-Z_] [a-zA-Z0-9_]*;
        for (i in 0 until cs.length) {
            val c = cs[i]
            if (i == 0) {
                if (!((c in 'a'..'z') || (c in 'A'..'Z') || c == '_')) {
                    return false
                }
            } else {
                if (!((c in 'a'..'z') || (c in 'A'..'Z') || (c in '0'..'9') || c == '_')) {
                    return false
                }
            }
        }
        return true
    }
}