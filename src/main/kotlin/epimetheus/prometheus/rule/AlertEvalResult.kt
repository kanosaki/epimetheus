package epimetheus.prometheus.rule

import epimetheus.model.Metric

class AlertEvalResult(val firedSince: Map<Metric, Long>, val timestamp: Long) {
    fun alertAndPendings(pendingUntil: Long): List<Pair<Metric, AlertStatus>> {
        return firedSince.map {
            val status = if ((timestamp - it.value) >= pendingUntil) {
                AlertStatus.FIRING
            } else {
                AlertStatus.PENDING
            }
            it.key to status
        }
    }
}
