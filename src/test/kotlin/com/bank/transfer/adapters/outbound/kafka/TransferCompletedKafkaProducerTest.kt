package com.bank.transfer.adapters.outbound.kafka

import com.bank.transfer.domain.event.TransferCompletedEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.math.BigDecimal
import java.time.Instant
import java.util.concurrent.CompletableFuture

class TransferCompletedKafkaProducerTest {

    private val kafkaTemplate: KafkaTemplate<String, String> = mockk()
    private val objectMapper = ObjectMapper().registerModule(JavaTimeModule())

    private lateinit var producer: TransferCompletedKafkaProducer

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        producer = TransferCompletedKafkaProducer(kafkaTemplate, objectMapper)
    }

    @Test
    fun `publish deve serializar TransferCompletedEvent e enviar para topico transfer-completed com chave transferId`() {
        val event = TransferCompletedEvent(
            transferId = "550e8400-e29b-41d4-a716-446655440000",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("150.75"),
            currency = "BRL",
            completedAt = Instant.parse("2025-01-15T10:30:00Z")
        )

        val sendResult = mockk<SendResult<String, String>>(relaxed = true)
        val future = CompletableFuture.completedFuture(sendResult)

        every { kafkaTemplate.send("transfer-completed", event.transferId, any()) } returns future

        producer.publish(event)

        verify(exactly = 1) {
            kafkaTemplate.send(
                "transfer-completed",
                event.transferId,
                match { payload ->
                    payload.contains("\"transferId\":\"550e8400-e29b-41d4-a716-446655440000\"") &&
                            payload.contains("\"sourceAccountId\":\"acc-123\"") &&
                            payload.contains("\"destinationAccountId\":\"acc-456\"") &&
                            payload.contains("\"amount\":150.75") &&
                            payload.contains("\"currency\":\"BRL\"")
                }
            )
        }
    }

    @Test
    fun `publish deve tratar falha assincrona do Kafka sem propagar excecao`() {
        val event = TransferCompletedEvent(
            transferId = "tx-fail-kafka",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("10.00"),
            currency = "BRL",
            completedAt = Instant.now()
        )

        val failedFuture = CompletableFuture<SendResult<String, String>>().apply {
            completeExceptionally(RuntimeException("Broker indisponivel"))
        }

        every { kafkaTemplate.send(any(), any(), any()) } returns failedFuture

        assertDoesNotThrow {
            producer.publish(event)
        }

        verify(exactly = 1) {
            kafkaTemplate.send("transfer-completed", event.transferId, any())
        }
    }
}
