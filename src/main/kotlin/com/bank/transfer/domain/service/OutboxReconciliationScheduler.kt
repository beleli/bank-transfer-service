package com.bank.transfer.domain.service

import com.bank.transfer.domain.event.TransferCompletedEvent
import com.bank.transfer.domain.port.MetricsPort
import com.bank.transfer.domain.port.TransactionRepositoryPort
import com.bank.transfer.domain.port.TransferCompletedProducerPort
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class OutboxReconciliationScheduler(
    private val transactionRepository: TransactionRepositoryPort,
    private val completedProducer: TransferCompletedProducerPort,
    private val metricsService: MetricsPort
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @Scheduled(
        fixedDelayString = "\${app.outbox.reconciliation-delay-ms:30000}",
        initialDelayString = "\${app.outbox.initial-delay-ms:10000}"
    )
    @SchedulerLock(
        name = "outbox_reconciliation",
        lockAtMostFor = "25s",
        lockAtLeastFor = "5s"
    )
    fun reconcileUnpublishedTransfers() {
        val pendingTransfers = try {
            transactionRepository.findUnpublished(limit = 50)
        } catch (e: Exception) {
            logger.error("Falha ao buscar transações pendentes de publicação no Outbox: ${e.message}", e)
            return
        }

        if (pendingTransfers.isEmpty()) {
            return
        }

        logger.info("Outbox Reconciler: Encontradas ${pendingTransfers.size} transferências pendentes de publicação no Kafka.")

        for (tx in pendingTransfers) {
            try {
                logger.info("Outbox Reconciler: Publicando evento pendente para transferId=${tx.transferId}")
                completedProducer.publish(
                    TransferCompletedEvent(
                        transferId = tx.transferId,
                        sourceAccountId = tx.sourceAccountId,
                        destinationAccountId = tx.destinationAccountId,
                        amount = tx.amount,
                        currency = tx.currency,
                        completedAt = tx.completedAt ?: Instant.now()
                    )
                )
                transactionRepository.markAsPublished(tx.transferId)
                logger.info("Outbox Reconciler: Transferência transferId=${tx.transferId} publicada e marcada como sincronizada com sucesso.")
            } catch (e: Exception) {
                logger.error("Outbox Reconciler: Falha ao publicar evento pendente para transferId=${tx.transferId}: ${e.message}", e)
                metricsService.recordFailure("outbox_reconciliation_failed", 0)
            }
        }
    }
}
