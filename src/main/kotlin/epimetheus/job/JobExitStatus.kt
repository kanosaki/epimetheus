package epimetheus.job

interface JobExitStatus {
    object Done: JobExitStatus
    object Terminate: JobExitStatus
}




