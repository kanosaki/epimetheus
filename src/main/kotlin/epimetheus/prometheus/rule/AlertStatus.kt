package epimetheus.prometheus.rule

enum class AlertStatus(val repr: String) {
    OK("ok"),
    PENDING("pending"),
    FIRING("firing"),
    ERROR("error")
}
