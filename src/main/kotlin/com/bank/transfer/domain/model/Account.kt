package com.bank.transfer.domain.model

import java.math.BigDecimal

data class Account(
    val accountId: String,
    val balance: BigDecimal,
    val currency: String = "BRL",
    val status: AccountStatus = AccountStatus.ACTIVE,
    val customerName: String
)
