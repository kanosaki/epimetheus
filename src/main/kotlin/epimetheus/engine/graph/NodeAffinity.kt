package epimetheus.engine.graph

import epimetheus.model.Metric

interface NodeAffinity {
    object Any : NodeAffinity // constant, meta nodes
    data class Single(val metric: Metric) : NodeAffinity // data collecting node
    object Splitted : NodeAffinity // aggregation node
}

