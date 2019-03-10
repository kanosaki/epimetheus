package epimetheus.job

data class Job(val fn: JobRunnable, val intervalMsec: Long, val nextExec: Long, val lastSucceed: Boolean? = null, val lastMessage: String? = null, val terminated: Boolean = false)
