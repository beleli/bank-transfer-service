package com.bank.transfer.adapters.outbound.kafka

import com.bank.transfer.domain.event.TransferCompletedEvent
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.port.TransferCompletedProducerPort
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class TransferCompletedKafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    @Value("\${app.kafka.topic.transfer-completed:transfer-completed}")
    private val topicName: String = "transfer-completed",
    @Value("\${app.kafka.producer.timeout-seconds:5}")
    private val timeoutSeconds: Long = 5
) : TransferCompletedProducerPort {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun publish(event: TransferCompletedEvent) {
        val payload = objectMapper.writeValueAsString(event)
        try {
            kafkaTemplate.send(topicName, event.transferId, payload)
                .get(timeoutSeconds, TimeUnit.SECONDS)
            logger.info("Publicado evento de conclusão no Kafka: topic=$topicName transferId=${event.transferId}")
        } catch (e: Exception) {
            logger.error("Falha ao publicar no Kafka topic=$topicName transferId=${event.transferId}: ${e.message}", e)
            throw TransientException("Falha ao publicar evento de conclusão no Kafka para transferId=${event.transferId}", e)
        }
    }
}
