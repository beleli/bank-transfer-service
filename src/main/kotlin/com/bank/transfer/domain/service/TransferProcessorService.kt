package com.bank.transfer.domain.service

import com.bank.transfer.domain.event.TransferCompletedEvent
import com.bank.transfer.domain.event.TransferFailedEvent
import com.bank.transfer.domain.exception.BusinessException
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.model.TransactionStatus
import com.bank.transfer.domain.model.TransferRequest
import com.bank.transfer.domain.port.*
import org.slf4j.LoggerFactory
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Recover
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class TransferProcessorService(
    private val accountRepository: AccountRepositoryPort,
    private val transactionRepository: TransactionRepositoryPort,
    private val validationService: TransferValidationService,
    private val completedProducer: TransferCompletedProducerPort,
    private val dlqProducer: TransferDlqProducerPort,
    private val metricsService: MetricsPort
) : ProcessTransferUseCase {

    private val logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private const val DEFAULT_REJECTION_REASON = "Regra de negócio violada"
        private const val METRIC_KAFKA_RETRY_EXHAUSTED = "kafka_publish_retry_exhausted"
        private const val METRIC_DLQ_RETRY_EXHAUSTED = "dlq_publish_retry_exhausted"
        private const val METRIC_TRANSIENT_RETRY_EXHAUSTED = "transient_retry_exhausted"
    }

    /**
     * Processa a transferência bancária com retry exponencial para erros transientes
     * e tratamento imediato sem retry para erros de negócio.
     */
    @Retryable(
        retryFor = [TransientException::class],
        noRetryFor = [BusinessException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 500, multiplier = 2.0)
    )
    override fun processTransfer(request: TransferRequest) {
        val startTime = System.currentTimeMillis()
        logger.info("Iniciando processamento da transferência transferId=${request.transferId} de=${request.sourceAccountId} para=${request.destinationAccountId} valor=${request.amount}")

        try {
            // 1. Verificação de Idempotência prévia
            val existingTx = transactionRepository.findById(request.transferId)
            if (existingTx != null) {
                handleExistingTransaction(existingTx, startTime)
                return
            }

            // 2. Consulta das contas envolvidas
            val sourceAccount = accountRepository.findById(request.sourceAccountId)
            val destinationAccount = accountRepository.findById(request.destinationAccountId)

            // 3. Validação de negócio
            validationService.validate(request, sourceAccount, destinationAccount)

            // 4. Execução Atômica e Idempotente no repositório de contas
            val completedRecord = request.toCompletedTransaction()
            accountRepository.executeAtomicTransfer(
                sourceAccountId = request.sourceAccountId,
                destinationAccountId = request.destinationAccountId,
                amount = request.amount,
                transaction = completedRecord
            )

            // 5. Publicação de evento de sucesso e finalização
            publishCompletedAndMark(completedRecord)

            val duration = elapsedTime(startTime)
            metricsService.recordSuccess(duration)
            logger.info("Transferência transferId=${request.transferId} processada com sucesso em ${duration}ms")

        } catch (e: BusinessException) {
            handleBusinessError(request, e, elapsedTime(startTime))
        }
    }

    /**
     * Trata transações já existentes no banco de dados, garantindo idempotência e reprocessamento
     * de publicações que falharam anteriormente (padrão Transactional Outbox).
     */
    private fun handleExistingTransaction(existingTx: Transaction, startTime: Long) {
        if (existingTx.status == TransactionStatus.COMPLETED && !existingTx.published) {
            logger.info("Transferência transferId=${existingTx.transferId} já concluída no banco, mas com publicação pendente no Kafka. Reenviando evento...")
            publishCompletedAndMark(existingTx)
            val duration = elapsedTime(startTime)
            metricsService.recordSuccess(duration)
            logger.info("Evento de conclusão reenviado com sucesso para transferId=${existingTx.transferId}")
            return
        }

        if (existingTx.status == TransactionStatus.FAILED && !existingTx.published) {
            logger.info("Transferência transferId=${existingTx.transferId} rejeitada anteriormente, mas com envio pendente para SQS DLQ. Reenviando...")
            val reason = existingTx.rejectionReason ?: DEFAULT_REJECTION_REASON
            publishFailedAndMark(existingTx.toFailedEvent(reason))
            val duration = elapsedTime(startTime)
            metricsService.recordFailure(reason, duration)
            logger.info("Evento de falha reenviado com sucesso para SQS DLQ transferId=${existingTx.transferId}")
            return
        }

        logger.warn("Transferência transferId=${existingTx.transferId} já processada anteriormente com status=${existingTx.status}. Ignorando duplicata (idempotência).")
    }

    /**
     * Tratamento de falhas de negócio:
     * - Não realiza retry
     * - Persiste transação como FAILED no repositório com motivo
     * - Envia mensagem detalhada para a fila SQS DLQ transfer-failed
     * - Registra métricas de falha
     */
    private fun handleBusinessError(request: TransferRequest, exception: BusinessException, duration: Long) {
        if (exception is BusinessException.DuplicateTransferException) {
            logger.warn("Transferência transferId=${request.transferId} detectada como duplicata no DynamoDB (idempotência). Ignorando reprocessamento.")
            return
        }

        val reason = exception.message ?: DEFAULT_REJECTION_REASON
        logger.warn("Transferência transferId=${request.transferId} rejeitada por regra de negócio: $reason")

        val failedRecord = request.toFailedTransaction(reason)
        val persisted = persistFailedTransactionSafely(failedRecord, "por requisição concorrente")
        if (!persisted) {
            return
        }

        publishFailedAndMark(failedRecord.toFailedEvent(reason))
        metricsService.recordFailure(reason, duration)
    }

    /**
     * Fallback/Recovery executado após esgotamento das tentativas de retry para TransientException.
     */
    @Recover
    fun recoverFromTransientError(exception: TransientException, request: TransferRequest) {
        val existingTx = transactionRepository.findById(request.transferId)
        if (existingTx?.status == TransactionStatus.COMPLETED) {
            val reason = "Falha ao publicar no Kafka após retries para transferência já COMPLETED: ${exception.message}"
            logger.error("A transferência transferId=${request.transferId} já foi executada, porém a publicação no Kafka falhou após esgotamento de retries. Mantendo COMPLETED com published=false para reconciliação: $reason", exception)
            metricsService.recordFailure(METRIC_KAFKA_RETRY_EXHAUSTED, 0)
            return
        }
        if (existingTx?.status == TransactionStatus.FAILED) {
            val reason = "Falha ao enviar para SQS DLQ após retries para transferência já FAILED: ${exception.message}"
            logger.error("A transferência transferId=${request.transferId} já foi registrada como FAILED, porém o envio para SQS DLQ falhou após esgotamento de retries. Mantendo FAILED com published=false para reconciliação: $reason", exception)
            metricsService.recordFailure(METRIC_DLQ_RETRY_EXHAUSTED, 0)
            return
        }

        val reason = "Esgotado limite de retries para falha transiente: ${exception.message}"
        logger.error("Falha irrecuperável por erro transiente após retries para transferId=${request.transferId}: $reason", exception)

        val failedRecord = request.toFailedTransaction(reason)
        val persisted = persistFailedTransactionSafely(failedRecord, "no recover")
        if (!persisted) {
            return
        }

        dlqProducer.sendToDlq(failedRecord.toFailedEvent(reason))
        metricsService.recordFailure(METRIC_TRANSIENT_RETRY_EXHAUSTED, 0)
    }

    /**
     * Persiste o registro de falha tratando condição de corrida no DynamoDB.
     * Retorna false se o registro já existir (duplicata concorrente), ou true caso contrário.
     */
    private fun persistFailedTransactionSafely(failedRecord: Transaction, logContext: String): Boolean {
        return try {
            transactionRepository.save(failedRecord)
            true
        } catch (_: BusinessException.DuplicateTransferException) {
            logger.warn("Transferência transferId=${failedRecord.transferId} já registrada no DynamoDB $logContext. Ignorando envio para DLQ.")
            false
        } catch (ex: Exception) {
            logger.error("Erro ao persistir status FAILED $logContext para transferId=${failedRecord.transferId}: ${ex.message}", ex)
            true
        }
    }

    private fun publishCompletedAndMark(transaction: Transaction) {
        completedProducer.publish(transaction.toCompletedEvent())
        transactionRepository.markAsPublished(transaction.transferId)
    }

    private fun publishFailedAndMark(event: TransferFailedEvent) {
        dlqProducer.sendToDlq(event)
        transactionRepository.markAsPublished(event.transferId)
    }

    private fun elapsedTime(startTime: Long): Long = System.currentTimeMillis() - startTime

    private fun Transaction.toCompletedEvent() = TransferCompletedEvent(
        transferId = transferId,
        sourceAccountId = sourceAccountId,
        destinationAccountId = destinationAccountId,
        amount = amount,
        currency = currency,
        completedAt = completedAt ?: Instant.now()
    )

    private fun Transaction.toFailedEvent(fallbackReason: String = DEFAULT_REJECTION_REASON) = TransferFailedEvent(
        transferId = transferId,
        sourceAccountId = sourceAccountId,
        destinationAccountId = destinationAccountId,
        amount = amount,
        currency = currency,
        reason = rejectionReason ?: fallbackReason
    )

    private fun TransferRequest.toCompletedTransaction(completedAt: Instant = Instant.now()) = Transaction(
        transferId = transferId,
        sourceAccountId = sourceAccountId,
        destinationAccountId = destinationAccountId,
        amount = amount,
        currency = currency,
        status = TransactionStatus.COMPLETED,
        createdAt = requestedAt,
        completedAt = completedAt,
        published = false
    )

    private fun TransferRequest.toFailedTransaction(reason: String, completedAt: Instant = Instant.now()) = Transaction(
        transferId = transferId,
        sourceAccountId = sourceAccountId,
        destinationAccountId = destinationAccountId,
        amount = amount,
        currency = currency,
        status = TransactionStatus.FAILED,
        rejectionReason = reason,
        createdAt = requestedAt,
        completedAt = completedAt,
        published = false
    )
}
