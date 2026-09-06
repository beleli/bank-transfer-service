package com.bank.transfer.adapters.outbound.dynamodb.mapper

import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.AccountStatus
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.math.BigDecimal

fun Map<String, AttributeValue>.toDomainAccount(defaultAccountId: String = ""): Account {
    return Account(
        accountId = this["accountId"]?.s() ?: defaultAccountId,
        balance = BigDecimal(this["balance"]?.n() ?: "0.00"),
        currency = this["currency"]?.s() ?: "BRL",
        status = AccountStatus.valueOf(this["status"]?.s() ?: AccountStatus.ACTIVE.name),
        customerName = this["customerName"]?.s() ?: ""
    )
}

fun Account.toItem(): Map<String, AttributeValue> {
    return mapOf(
        "accountId" to AttributeValue.builder().s(this.accountId).build(),
        "balance" to AttributeValue.builder().n(this.balance.toPlainString()).build(),
        "currency" to AttributeValue.builder().s(this.currency).build(),
        "status" to AttributeValue.builder().s(this.status.name).build(),
        "customerName" to AttributeValue.builder().s(this.customerName).build()
    )
}
