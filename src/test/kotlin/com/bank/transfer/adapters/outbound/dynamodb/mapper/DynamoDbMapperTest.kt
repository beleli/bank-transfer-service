package com.bank.transfer.adapters.outbound.dynamodb.mapper

import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.AccountStatus
import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.model.TransactionStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class DynamoDbMapperTest {

    @Test
    fun `deve mapear Account para AttributeValue map e vice-versa via extension functions`() {
        val account = Account(
            accountId = "acc-100",
            balance = BigDecimal("1500.50"),
            currency = "BRL",
            status = AccountStatus.ACTIVE,
            customerName = "Ana Maria"
        )

        val item = account.toItem()
        assertEquals("acc-100", item["accountId"]?.s())
        assertEquals("1500.50", item["balance"]?.n())
        assertEquals("BRL", item["currency"]?.s())
        assertEquals("ACTIVE", item["status"]?.s())
        assertEquals("Ana Maria", item["customerName"]?.s())

        val domainAccount = item.toDomainAccount()
        assertEquals(account.accountId, domainAccount.accountId)
        assertEquals(account.balance, domainAccount.balance)
        assertEquals(account.currency, domainAccount.currency)
        assertEquals(account.status, domainAccount.status)
        assertEquals(account.customerName, domainAccount.customerName)
    }

    @Test
    fun `deve mapear TransactionRecord para AttributeValue map e vice-versa via extension functions`() {
        val now = Instant.now()
        val record = Transaction(
            transferId = "tx-999",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("75.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            rejectionReason = null,
            createdAt = now,
            completedAt = now
        )

        val item = record.toItem()
        assertEquals("tx-999", item["transferId"]?.s())
        assertEquals("acc-1", item["sourceAccountId"]?.s())
        assertEquals("acc-2", item["destinationAccountId"]?.s())
        assertEquals("75.00", item["amount"]?.n())
        assertEquals("BRL", item["currency"]?.s())
        assertEquals("COMPLETED", item["status"]?.s())
        assertEquals(false, item["published"]?.bool())
        assertNotNull(item["createdAt"]?.s())
        assertNotNull(item["completedAt"]?.s())

        val domainRecord = item.toDomainTransactionRecord()
        assertEquals(record.transferId, domainRecord.transferId)
        assertEquals(record.sourceAccountId, domainRecord.sourceAccountId)
        assertEquals(record.destinationAccountId, domainRecord.destinationAccountId)
        assertEquals(record.amount, domainRecord.amount)
        assertEquals(record.currency, domainRecord.currency)
        assertEquals(record.status, domainRecord.status)
        assertEquals(record.published, domainRecord.published)
    }
}
