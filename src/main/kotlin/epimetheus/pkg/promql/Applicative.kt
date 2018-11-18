package epimetheus.pkg.promql


interface Applicative {
    val argTypes: List<ValueType>
    val returnType: ValueType
    val variadic: Boolean
}