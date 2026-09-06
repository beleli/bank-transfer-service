package com.bank.transfer.adapters.outbound.metrics

import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class MicrometerMetricsAdapterTest {

    private lateinit var meterRegistry: SimpleMeterRegistry
    private lateinit var timer: Timer
    private lateinit var metricsAdapter: MicrometerMetricsAdapter

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        timer = Timer.builder("bank.transfers.duration")
            .description("Duration of transfer processing")
            .register(meterRegistry)
        metricsAdapter = MicrometerMetricsAdapter(meterRegistry, timer)
    }

    @Test
    fun `recordSuccess deve incrementar contador de sucesso e registrar duracao no timer`() {
        metricsAdapter.recordSuccess(150L)

        val successCount = meterRegistry.get("bank.transfers.processed")
            .tag("status", "completed")
            .counter()
            .count()

        assertEquals(1.0, successCount)
        assertEquals(1, timer.count())
        assertEquals(150.0, timer.totalTime(TimeUnit.MILLISECONDS))
    }

    @Test
    fun `recordFailure deve incrementar contador de falha, detalhe da razao e duracao no timer`() {
        val reason = "INSUFFICIENT_BALANCE"
        metricsAdapter.recordFailure(reason, 80L)

        val failureCount = meterRegistry.get("bank.transfers.processed")
            .tag("status", "failed")
            .counter()
            .count()

        val detailCount = meterRegistry.get("bank.transfers.failed.detail")
            .tag("reason", reason)
            .counter()
            .count()

        assertEquals(1.0, failureCount)
        assertEquals(1.0, detailCount)
        assertEquals(1, timer.count())
        assertEquals(80.0, timer.totalTime(TimeUnit.MILLISECONDS))
    }

    @Test
    fun `recordFailure deve truncar razoes longas para no maximo 50 caracteres`() {
        val longReason = "A".repeat(80)
        metricsAdapter.recordFailure(longReason, 45L)

        val detailCount = meterRegistry.get("bank.transfers.failed.detail")
            .tag("reason", "A".repeat(50))
            .counter()
            .count()

        assertEquals(1.0, detailCount)
    }
}
