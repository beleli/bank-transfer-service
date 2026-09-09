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
                if (existingTx.status == TransactionStatus.COMPLETED && !existingTx.published) {
                    logger.info("Transferência transferId=${request.transferId} já concluída no banco, mas com publicação pendente no Kafka. Reenviando evento...")
                    completedProducer.publish(
                        TransferCompletedEvent(
                            transferId = existingTx.transferId,
                            sourceAccountId = existingTx.sourceAccountId,
                            destinationAccountId = existingTx.destinationAccountId,
                            amount = existingTx.amount,
                            currency = existingTx.currency,
                            completedAt = existingTx.completedAt ?: Instant.now()
                        )
                    )
                    transactionRepository.markAsPublished(existingTx.transferId)
                    val duration = System.currentTimeMillis() - startTime
                    metricsService.recordSuccess(duration)
                    logger.info("Evento de conclusão reenviado com sucesso para transferId=${existingTx.transferId}")
                    return
                }
                logger.warn("Transferência transferId=${request.transferId} já processada anteriormente com status=${existingTx.status}. Ignorando duplicata (idempotência).")
                return
            }

            // 2. Consulta das contas envolvidas
            val sourceAccount = accountRepository.findById(request.sourceAccountId)
            val destinationAccount = accountRepository.findById(request.destinationAccountId)

            // 3. Validação de negócio
            validationService.validate(request, sourceAccount, destinationAccount)

            // 4. Execução Atômica e Idempotente no repositório de contas
            val completedRecord = Transaction(
                transferId = request.transferId,
                sourceAccountId = request.sourceAccountId,
                destinationAccountId = request.destinationAccountId,
                amount = request.amount,
                currency = request.currency,
                status = TransactionStatus.COMPLETED,
                createdAt = request.requestedAt,
                completedAt = Instant.now(),
                published = false
            )

            accountRepository.executeAtomicTransfer(
                sourceAccountId = request.sourceAccountId,
                destinationAccountId = request.destinationAccountId,
                amount = request.amount,
                transaction = completedRecord
            )

            // 5. Publicação de evento de sucesso
            completedProducer.publish(
                TransferCompletedEvent(
                    transferId = request.transferId,
                    sourceAccountId = request.sourceAccountId,
                    destinationAccountId = request.destinationAccountId,
                    amount = request.amount,
                    currency = request.currency,
                    completedAt = completedRecord.completedAt ?: Instant.now()
                )
            )
            transactionRepository.markAsPublished(request.transferId)

            val duration = System.currentTimeMillis() - startTime
            metricsService.recordSuccess(duration)
            logger.info("Transferência transferId=${request.transferId} processada com sucesso em ${duration}ms")

        } catch (e: BusinessException) {
            val duration = System.currentTimeMillis() - startTime
            handleBusinessError(request, e, duration)
        }
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
            logger.warn("Transferência transferId=${request.transferId} detectada como duplicata 985466" +
                    " no DynamoDB (idempotência). Ignorando reprocessamento.")
            return
        }

        val reason = exception.message ?: "Regra de negócio violada"
        logger.warn("Transferência transferId=${request.transferId} rejeitada por regra de negócio: $reason")

        val failedRecord = Transaction(
            transferId = request.transferId,
            sourceAccountId = request.sourceAccountId,
            destinationAccountId = request.destinationAccountId,
            amount = request.amount,
            currency = request.currency,
            status = TransactionStatus.FAILED,
            rejectionReason = reason,
            createdAt = request.requestedAt,
            completedAt = Instant.now()
        )
        try {
            transactionRepository.save(failedRecord)
        } catch (ex: Exception) {
            logger.error("Erro ao persistir status FAILED para transferId=${request.transferId}: ${ex.message}", ex)
        }

        dlqProducer.sendToDlq(
            TransferFailedEvent(
                transferId = request.transferId,
                sourceAccountId = request.sourceAccountId,
                destinationAccountId = request.destinationAccountId,
                amount = request.amount,
                currency = request.currency,
                reason = reason
            )
        )

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
            metricsService.recordFailure("kafka_publish_retry_exhausted", 0)
            return
        }

        val reason = "Esgotado limite de retries para falha transiente: ${exception.message}"
        logger.error("Falha irrecuperável por erro transiente após retries para transferId=${request.transferId}: $reason", exception)

        val failedRecord = Transaction(
            transferId = request.transferId,
            sourceAccountId = request.sourceAccountId,
            destinationAccountId = request.destinationAccountId,
            amount = request.amount,
            currency = request.currency,
            status = TransactionStatus.FAILED,
            rejectionReason = reason,
            createdAt = request.requestedAt,
            completedAt = Instant.now()
        )
        try {
            transactionRepository.save(failedRecord)
        } catch (ex: Exception) {
            logger.error("Erro ao persistir status FAILED no recover para transferId=${request.transferId}: ${ex.message}", ex)
        }

        dlqProducer.sendToDlq(
            TransferFailedEvent(
                transferId = request.transferId,
                sourceAccountId = request.sourceAccountId,
                destinationAccountId = request.destinationAccountId,
                amount = request.amount,
                currency = request.currency,
                reason = reason
            )
        )

        metricsService.recordFailure("transient_retry_exhausted", 0)
    }
}
