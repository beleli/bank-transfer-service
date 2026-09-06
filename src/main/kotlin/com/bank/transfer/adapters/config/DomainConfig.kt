package com.bank.transfer.adapters.config

import com.bank.transfer.domain.service.TransferValidationService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class DomainConfig {

    @Bean
    fun transferValidationService(): TransferValidationService {
        return TransferValidationService()
    }
}
