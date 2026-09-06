package com.bank.transfer.domain.model

import java.math.BigDecimal
import java.time.Instant

data class Transaction(
    val transferId: String,
    val sourceAccountId: String,
    val destinationAccountId: String,
    val amount: BigDecimal,
    val currency: String,
    val status: TransactionStatus,
    val createdAt: Instant,
    val rejectionReason: String? = null,
    val completedAt: Instant? = null
)
