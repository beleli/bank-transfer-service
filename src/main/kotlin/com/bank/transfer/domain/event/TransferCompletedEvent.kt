package com.bank.transfer.domain.event

import java.math.BigDecimal
import java.time.Instant

data class TransferCompletedEvent(
    val transferId: String,
    val sourceAccountId: String,
    val destinationAccountId: String,
    val amount: BigDecimal,
    val currency: String,
    val completedAt: Instant = Instant.now()
)
