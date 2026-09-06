package com.bank.transfer.adapters.outbound.kafka

import com.bank.transfer.domain.event.TransferCompletedEvent
import com.bank.transfer.domain.port.TransferCompletedProducerPort
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class TransferCompletedKafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) : TransferCompletedProducerPort {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private val topicName = "transfer-completed"

    override fun publish(event: TransferCompletedEvent) {
        val payload = objectMapper.writeValueAsString(event)
        kafkaTemplate.send(topicName, event.transferId, payload)
            .whenComplete { _, ex ->
                if (ex == null) {
                    logger.info("Publicado evento de conclusão no Kafka: topic=$topicName transferId=${event.transferId}")
                } else {
                    logger.error("Falha ao publicar no Kafka topic=$topicName transferId=${event.transferId}", ex)
                }
            }
    }
}
