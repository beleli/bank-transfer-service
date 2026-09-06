package com.bank.transfer.domain.port

interface MetricsPort {
    fun recordSuccess(durationMillis: Long)
    fun recordFailure(reason: String, durationMillis: Long)
}
