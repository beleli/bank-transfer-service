package com.bank.transfer.adapters.inbound.kafka

import com.bank.transfer.adapters.inbound.kafka.avro.TransferRequestEvent
import com.bank.transfer.adapters.inbound.kafka.mapper.toDomain
import com.bank.transfer.domain.event.TransferFailedEvent
import com.bank.transfer.domain.port.ProcessTransferUseCase
import com.bank.transfer.domain.port.TransferDlqProducerPort
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.UUID

@Component
class TransferConsumer(
    private val processTransferUseCase: ProcessTransferUseCase,
    private val dlqProducer: TransferDlqProducerPort
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        const val TOPIC_TRANSFER_REQUESTED = "\${app.kafka.topic.transfer-requested:transfer-requested}"
        const val CONSUMER_GROUP_ID = "\${spring.kafka.consumer.group-id:transfer-service}"
    }

    @KafkaListener(
        topics = [TOPIC_TRANSFER_REQUESTED],
        groupId = CONSUMER_GROUP_ID
    )
    fun handle(event: TransferRequestEvent) {
        var transferId: String? = null
        try {
            transferId = event.transferId
            val transfer = event.toDomain()

            // Configurar Correlation ID no MDC para logs estruturados
            MDC.put("transferId", transfer.transferId)
            MDC.put("correlationId", transfer.transferId)
            MDC.put("sourceAccountId", transfer.sourceAccountId)
            MDC.put("destinationAccountId", transfer.destinationAccountId)

            logger.info("Mensagem Avro recebida do tópico $TOPIC_TRANSFER_REQUESTED: transferId=${transfer.transferId}")

            processTransferUseCase.processTransfer(transfer)

        } catch (e: Exception) {
            logger.error("Erro inesperado ao processar evento Avro do Kafka: ${e.message}", e)
            val fallbackId = transferId ?: UUID.randomUUID().toString()
            dlqProducer.sendToDlq(
                TransferFailedEvent(
                    transferId = fallbackId,
                    sourceAccountId = event.sourceAccountId ?: "UNKNOWN",
                    destinationAccountId = event.destinationAccountId ?: "UNKNOWN",
                    amount = BigDecimal.ZERO,
                    currency = "BRL",
                    reason = "Processing error on Avro event: ${e.message}"
                )
            )
        } finally {
            MDC.clear()
        }
    }
}
