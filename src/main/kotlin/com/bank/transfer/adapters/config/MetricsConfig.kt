package com.bank.transfer.adapters.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MetricsConfig {

    @Bean
    fun transferDurationTimer(registry: MeterRegistry): Timer {
        return Timer.builder("bank.transfers.duration")
            .description("Time taken to process a bank transfer")
            .publishPercentileHistogram()
            .register(registry)
    }
}
