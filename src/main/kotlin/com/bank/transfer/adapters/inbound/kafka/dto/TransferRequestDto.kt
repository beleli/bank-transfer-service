package com.bank.transfer.adapters.inbound.kafka.dto

import jakarta.validation.constraints.DecimalMin
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import java.math.BigDecimal
import java.time.Instant

data class TransferRequestDto(
    @field:NotBlank(message = "transferId is required")
    val transferId: String? = null,

    @field:NotBlank(message = "sourceAccountId is required")
    val sourceAccountId: String? = null,

    @field:NotBlank(message = "destinationAccountId is required")
    val destinationAccountId: String? = null,

    @field:NotNull(message = "amount is required")
    @field:DecimalMin(value = "0.01", message = "amount must be positive")
    val amount: BigDecimal? = null,

    @field:NotBlank(message = "currency is required")
    val currency: String? = null,

    val requestedAt: Instant? = null
)
