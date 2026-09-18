package com.bank.transfer.adapters.outbound.dynamodb.mapper

import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.model.TransactionStatus
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal
import java.time.Instant

fun Map<String, AttributeValue>.toDomainTransactionRecord(defaultTransferId: String = ""): Transaction {
    return Transaction(
        transferId = this["transferId"]?.s() ?: defaultTransferId,
        sourceAccountId = this["sourceAccountId"]?.s() ?: "",
        destinationAccountId = this["destinationAccountId"]?.s() ?: "",
        amount = BigDecimal(this["amount"]?.n() ?: "0.00"),
        currency = this["currency"]?.s() ?: "BRL",
        status = TransactionStatus.valueOf(this["status"]?.s() ?: TransactionStatus.FAILED.name),
        rejectionReason = this["rejectionReason"]?.s(),
        createdAt = this["createdAt"]?.s()?.let { Instant.parse(it) } ?: Instant.now(),
        completedAt = this["completedAt"]?.s()?.let { Instant.parse(it) },
        published = this["published"]?.bool() ?: false
    )
}

fun Transaction.toItem(): Map<String, AttributeValue> {
    val item = mutableMapOf(
        "transferId" to AttributeValue.builder().s(this.transferId).build(),
        "sourceAccountId" to AttributeValue.builder().s(this.sourceAccountId).build(),
        "destinationAccountId" to AttributeValue.builder().s(this.destinationAccountId).build(),
        "amount" to AttributeValue.builder().n(this.amount.toPlainString()).build(),
        "currency" to AttributeValue.builder().s(this.currency).build(),
        "status" to AttributeValue.builder().s(this.status.name).build(),
        "createdAt" to AttributeValue.builder().s(this.createdAt.toString()).build(),
        "published" to AttributeValue.builder().bool(this.published).build()
    )

    this.rejectionReason?.let {
        item["rejectionReason"] = AttributeValue.builder().s(it).build()
    }
    this.completedAt?.let {
        item["completedAt"] = AttributeValue.builder().s(it.toString()).build()
    }

    return item
}
