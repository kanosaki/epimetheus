package epimetheus.prometheus.rule

import epimetheus.model.Metric

class AlertStatus(val firedSince: Map<Metric, Long>) {
}
