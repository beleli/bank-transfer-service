package com.bank.transfer.domain.model

import java.math.BigDecimal
import java.time.Instant

data class TransferRequest(
    val transferId: String,
    val sourceAccountId: String,
    val destinationAccountId: String,
    val amount: BigDecimal,
    val currency: String,
    val requestedAt: Instant = Instant.now()
)
