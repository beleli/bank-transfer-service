package com.bank.transfer.domain.service

import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.model.TransactionStatus
import com.bank.transfer.domain.port.MetricsPort
import com.bank.transfer.domain.port.TransactionRepositoryPort
import com.bank.transfer.domain.port.TransferCompletedProducerPort
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class OutboxReconciliationSchedulerTest {

    private val transactionRepository: TransactionRepositoryPort = mockk(relaxed = true)
    private val completedProducer: TransferCompletedProducerPort = mockk(relaxed = true)
    private val metricsService: MetricsPort = mockk(relaxed = true)

    private lateinit var scheduler: OutboxReconciliationScheduler

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        scheduler = OutboxReconciliationScheduler(
            transactionRepository,
            completedProducer,
            metricsService
        )
    }

    @Test
    fun `deve buscar transferencias nao publicadas e reenviar com sucesso marcando como publicadas`() {
        val tx1 = Transaction(
            transferId = "tx-1",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = false
        )
        val tx2 = Transaction(
            transferId = "tx-2",
            sourceAccountId = "acc-3",
            destinationAccountId = "acc-4",
            amount = BigDecimal("250.50"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = false
        )

        every { transactionRepository.findUnpublished(limit = 50) } returns listOf(tx1, tx2)

        scheduler.reconcileUnpublishedTransfers()

        verify(exactly = 1) {
            completedProducer.publish(match { it.transferId == "tx-1" && it.amount == BigDecimal("100.00") })
        }
        verify(exactly = 1) {
            transactionRepository.markAsPublished("tx-1")
        }
        verify(exactly = 1) {
            completedProducer.publish(match { it.transferId == "tx-2" && it.amount == BigDecimal("250.50") })
        }
        verify(exactly = 1) {
            transactionRepository.markAsPublished("tx-2")
        }
    }

    @Test
    fun `deve encerrar sem acao quando lista de pendentes for vazia`() {
        every { transactionRepository.findUnpublished(limit = 50) } returns emptyList()

        scheduler.reconcileUnpublishedTransfers()

        verify(exactly = 0) { completedProducer.publish(any()) }
        verify(exactly = 0) { transactionRepository.markAsPublished(any()) }
    }

    @Test
    fun `deve continuar processando outras transferencias mesmo quando uma falhar na publicacao`() {
        val txFail = Transaction(
            transferId = "tx-fail",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = false
        )
        val txSuccess = Transaction(
            transferId = "tx-ok",
            sourceAccountId = "acc-3",
            destinationAccountId = "acc-4",
            amount = BigDecimal("200.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = false
        )

        every { transactionRepository.findUnpublished(limit = 50) } returns listOf(txFail, txSuccess)
        every { completedProducer.publish(match { it.transferId == "tx-fail" }) } throws RuntimeException("Kafka timeout")

        scheduler.reconcileUnpublishedTransfers()

        // Primeira transação falhou ao publicar, logo NÃO deve marcar como publicada
        verify(exactly = 0) { transactionRepository.markAsPublished("tx-fail") }
        verify(exactly = 1) { metricsService.recordFailure("outbox_reconciliation_failed", 0) }

        // Segunda transação deve ter sido processada com sucesso
        verify(exactly = 1) { completedProducer.publish(match { it.transferId == "tx-ok" }) }
        verify(exactly = 1) { transactionRepository.markAsPublished("tx-ok") }
    }

    @Test
    fun `deve capturar erro ao consultar repositorio sem quebrar execucao do scheduler`() {
        every { transactionRepository.findUnpublished(limit = 50) } throws RuntimeException("DynamoDB down")

        scheduler.reconcileUnpublishedTransfers()

        verify(exactly = 0) { completedProducer.publish(any()) }
        verify(exactly = 0) { transactionRepository.markAsPublished(any()) }
    }
}
