package com.bank.transfer.domain.port

import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.Transaction
import java.math.BigDecimal

interface AccountRepositoryPort {
    fun findById(accountId: String): Account?
    fun save(account: Account)
    fun executeAtomicTransfer(
        sourceAccountId: String,
        destinationAccountId: String,
        amount: BigDecimal,
        transaction: Transaction
    )
}
