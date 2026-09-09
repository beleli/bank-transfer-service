package com.bank.transfer.domain.port

import com.bank.transfer.domain.model.Transaction

interface TransactionRepositoryPort {
    fun findById(transferId: String): Transaction?
    fun save(record: Transaction)
    fun markAsPublished(transferId: String)
}
