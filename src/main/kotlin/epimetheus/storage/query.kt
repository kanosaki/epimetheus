package epimetheus.storage

import epimetheus.model.MetricMatcher

interface LabelPredicate

data class Query(val metricName: String, val labelsMatch: MetricMatcher)