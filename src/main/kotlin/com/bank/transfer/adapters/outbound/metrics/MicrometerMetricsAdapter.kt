package com.bank.transfer.adapters.outbound.metrics

import com.bank.transfer.domain.port.MetricsPort
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class MicrometerMetricsAdapter(
    private val meterRegistry: MeterRegistry,
    private val transferDurationTimer: Timer
) : MetricsPort {

    private val successCounter: Counter = Counter.builder("bank.transfers.processed")
        .tag("status", "completed")
        .description("Number of successfully completed bank transfers")
        .register(meterRegistry)

    private val failureCounter: Counter = Counter.builder("bank.transfers.processed")
        .tag("status", "failed")
        .description("Number of failed bank transfers")
        .register(meterRegistry)

    override fun recordSuccess(durationMillis: Long) {
        successCounter.increment()
        transferDurationTimer.record(durationMillis, TimeUnit.MILLISECONDS)
    }

    override fun recordFailure(reason: String, durationMillis: Long) {
        Counter.builder("bank.transfers.failed.detail")
            .tag("reason", reason.take(50))
            .register(meterRegistry)
            .increment()

        failureCounter.increment()
        transferDurationTimer.record(durationMillis, TimeUnit.MILLISECONDS)
    }
}
