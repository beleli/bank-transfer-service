package com.bank.transfer.domain.event

import java.math.BigDecimal
import java.time.Instant

data class TransferFailedEvent(
    val transferId: String,
    val sourceAccountId: String,
    val destinationAccountId: String,
    val amount: BigDecimal,
    val currency: String,
    val reason: String,
    val failedAt: Instant = Instant.now()
)
