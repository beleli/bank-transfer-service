package com.bank.transfer.adapters.inbound.kafka.mapper

import com.bank.transfer.adapters.inbound.kafka.dto.TransferRequestDto
import com.bank.transfer.adapters.inbound.kafka.avro.TransferRequestEvent
import com.bank.transfer.domain.model.TransferRequest
import java.math.BigDecimal
import java.time.Instant

import java.math.RoundingMode

fun TransferRequestDto.toDomain(): TransferRequest {
    requireNotNull(transferId) { "transferId cannot be null" }
    requireNotNull(sourceAccountId) { "sourceAccountId cannot be null" }
    requireNotNull(destinationAccountId) { "destinationAccountId cannot be null" }
    requireNotNull(amount) { "amount cannot be null" }
    requireNotNull(currency) { "currency cannot be null" }

    return TransferRequest(
        transferId = this.transferId,
        sourceAccountId = this.sourceAccountId,
        destinationAccountId = this.destinationAccountId,
        amount = this.amount,
        currency = this.currency,
        requestedAt = this.requestedAt ?: Instant.now()
    )
}

fun TransferRequestEvent.toDomain(): TransferRequest {
    requireNotNull(transferId) { "transferId cannot be null" }
    requireNotNull(sourceAccountId) { "sourceAccountId cannot be null" }
    requireNotNull(destinationAccountId) { "destinationAccountId cannot be null" }
    requireNotNull(currency) { "currency cannot be null" }

    val parsedInstant = if (!requestedAt.isNullOrBlank()) {
        runCatching { Instant.parse(requestedAt) }.getOrDefault(Instant.now())
    } else {
        Instant.now()
    }

    return TransferRequest(
        transferId = this.transferId,
        sourceAccountId = this.sourceAccountId,
        destinationAccountId = this.destinationAccountId,
        amount = BigDecimal.valueOf(this.amount).setScale(2, RoundingMode.HALF_UP),
        currency = this.currency,
        requestedAt = parsedInstant
    )
}
