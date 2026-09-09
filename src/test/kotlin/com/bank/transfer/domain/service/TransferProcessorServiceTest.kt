package com.bank.transfer.domain.service

import com.bank.transfer.domain.exception.BusinessException
import com.bank.transfer.domain.model.*
import com.bank.transfer.domain.port.*
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class TransferProcessorServiceTest {

    private val accountRepository: AccountRepositoryPort = mockk(relaxed = true)
    private val transactionRepository: TransactionRepositoryPort = mockk(relaxed = true)
    private val validationService: TransferValidationService = mockk(relaxed = true)
    private val completedProducer: TransferCompletedProducerPort = mockk(relaxed = true)
    private val dlqProducer: TransferDlqProducerPort = mockk(relaxed = true)
    private val metricsService: MetricsPort = mockk(relaxed = true)

    private lateinit var processorService: TransferProcessorService

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        processorService = TransferProcessorService(
            accountRepository,
            transactionRepository,
            validationService,
            completedProducer,
            dlqProducer,
            metricsService
        )
    }

    @Test
    fun `deve processar transferencia com sucesso e publicar no topico kafka`() {
        val request = TransferRequest(
            transferId = "550e8400-e29b-41d4-a716-446655440000",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("150.75"),
            currency = "BRL"
        )

        val sourceAccount = Account("acc-123", BigDecimal("5000.00"), "BRL", AccountStatus.ACTIVE, "João Silva")
        val destAccount = Account("acc-456", BigDecimal("1200.50"), "BRL", AccountStatus.ACTIVE, "Maria Santos")

        every { transactionRepository.findById(request.transferId) } returns null
        every { accountRepository.findById("acc-123") } returns sourceAccount
        every { accountRepository.findById("acc-456") } returns destAccount

        processorService.processTransfer(request)

        // Verifica que a transação atômica foi disparada
        verify(exactly = 1) {
            accountRepository.executeAtomicTransfer(
                "acc-123",
                "acc-456",
                BigDecimal("150.75"),
                match { it.transferId == request.transferId && it.status == TransactionStatus.COMPLETED }
            )
        }

        // Verifica que o evento de sucesso foi publicado
        verify(exactly = 1) {
            completedProducer.publish(match { it.transferId == request.transferId })
        }

        // Verifica que foi marcada como publicada
        verify(exactly = 1) {
            transactionRepository.markAsPublished(request.transferId)
        }

        // Não deve enviar para DLQ
        verify(exactly = 0) { dlqProducer.sendToDlq(any()) }

        // Verifica registro de métrica de sucesso
        verify(exactly = 1) { metricsService.recordSuccess(any()) }
    }

    @Test
    fun `deve garantir idempotencia e nao reprocessar quando transferId ja existir com published true`() {
        val request = TransferRequest(
            transferId = "tx-duplicada",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("50.00"),
            currency = "BRL"
        )

        val existingTx = Transaction(
            transferId = "tx-duplicada",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = true
        )

        every { transactionRepository.findById("tx-duplicada") } returns existingTx

        processorService.processTransfer(request)

        // Garante que NENHUMA operação de débito/crédito ou publicação ocorra
        verify(exactly = 0) { accountRepository.executeAtomicTransfer(any(), any(), any(), any()) }
        verify(exactly = 0) { completedProducer.publish(any()) }
        verify(exactly = 0) { dlqProducer.sendToDlq(any()) }
    }

    @Test
    fun `deve reenviar evento no Kafka quando transacao existir como COMPLETED mas published for false`() {
        val request = TransferRequest(
            transferId = "tx-retry-publish",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("50.00"),
            currency = "BRL"
        )

        val existingTx = Transaction(
            transferId = "tx-retry-publish",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = false
        )

        every { transactionRepository.findById("tx-retry-publish") } returns existingTx

        processorService.processTransfer(request)

        // NÃO deve debitar/creditar novamente
        verify(exactly = 0) { accountRepository.executeAtomicTransfer(any(), any(), any(), any()) }

        // DEVE reenviar para o Kafka
        verify(exactly = 1) { completedProducer.publish(match { it.transferId == "tx-retry-publish" }) }

        // DEVE marcar como publicada
        verify(exactly = 1) { transactionRepository.markAsPublished("tx-retry-publish") }

        // NÃO deve enviar para DLQ
        verify(exactly = 0) { dlqProducer.sendToDlq(any()) }
    }

    @Test
    fun `deve enviar para SQS DLQ e persistir status FAILED quando houver erro de negocio`() {
        val request = TransferRequest(
            transferId = "tx-saldo-insuficiente",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("99999.00"),
            currency = "BRL"
        )

        val sourceAccount = Account("acc-123", BigDecimal("50.00"), "BRL", AccountStatus.ACTIVE, "João Silva")
        val destAccount = Account("acc-456", BigDecimal("1200.50"), "BRL", AccountStatus.ACTIVE, "Maria Santos")

        every { transactionRepository.findById(request.transferId) } returns null
        every { accountRepository.findById("acc-123") } returns sourceAccount
        every { accountRepository.findById("acc-456") } returns destAccount
        every { validationService.validate(any(), any(), any()) } throws BusinessException.InsufficientBalanceException("acc-123", "50.00", "99999.00")

        processorService.processTransfer(request)

        // Não deve executar transferência atômica
        verify(exactly = 0) { accountRepository.executeAtomicTransfer(any(), any(), any(), any()) }

        // Não deve publicar sucesso
        verify(exactly = 0) { completedProducer.publish(any()) }

        // Deve persistir transação com status FAILED
        verify(exactly = 1) {
            transactionRepository.save(match {
                it.transferId == request.transferId && it.status == TransactionStatus.FAILED && it.rejectionReason != null
            })
        }

        // Deve enviar para SQS DLQ com motivo da falha
        verify(exactly = 1) {
            dlqProducer.sendToDlq(match {
                it.transferId == request.transferId && it.reason.contains("insufficient balance")
            })
        }

        // Registra métrica de falha
        verify(exactly = 1) { metricsService.recordFailure(any(), any()) }
    }

    @Test
    fun `deve tratar DuplicateTransferException na transacao atomica sem sobrescrever para FAILED nem enviar para DLQ`() {
        val request = TransferRequest(
            transferId = "tx-duplicada-concorrente",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("100.00"),
            currency = "BRL"
        )

        val sourceAccount = Account("acc-123", BigDecimal("5000.00"), "BRL", AccountStatus.ACTIVE, "João Silva")
        val destAccount = Account("acc-456", BigDecimal("1200.50"), "BRL", AccountStatus.ACTIVE, "Maria Santos")

        every { transactionRepository.findById(request.transferId) } returns null
        every { accountRepository.findById("acc-123") } returns sourceAccount
        every { accountRepository.findById("acc-456") } returns destAccount
        every {
            accountRepository.executeAtomicTransfer(any(), any(), any(), any())
        } throws BusinessException.DuplicateTransferException(request.transferId)

        processorService.processTransfer(request)

        // Não deve persistir status FAILED
        verify(exactly = 0) { transactionRepository.save(any()) }

        // Não deve enviar para DLQ
        verify(exactly = 0) { dlqProducer.sendToDlq(any()) }

        // Não deve publicar sucesso
        verify(exactly = 0) { completedProducer.publish(any()) }
    }

    @Test
    fun `recoverFromTransientError nao deve sobrescrever para FAILED nem enviar para DLQ se transacao ja for COMPLETED`() {
        val request = TransferRequest(
            transferId = "tx-recover-completed",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("50.00"),
            currency = "BRL"
        )

        val existingTx = Transaction(
            transferId = "tx-recover-completed",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            published = false
        )

        every { transactionRepository.findById("tx-recover-completed") } returns existingTx

        val transientEx = com.bank.transfer.domain.exception.TransientException("Kafka indisponivel")
        processorService.recoverFromTransientError(transientEx, request)

        // NÃO deve sobrescrever no banco como FAILED
        verify(exactly = 0) { transactionRepository.save(any()) }

        // NÃO deve enviar para DLQ de falha de negócio
        verify(exactly = 0) { dlqProducer.sendToDlq(any()) }

        // Deve registrar métrica de retry esgotado
        verify(exactly = 1) { metricsService.recordFailure("kafka_publish_retry_exhausted", 0) }
    }
}
