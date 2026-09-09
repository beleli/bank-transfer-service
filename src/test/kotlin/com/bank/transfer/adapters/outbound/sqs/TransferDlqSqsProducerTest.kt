package com.bank.transfer.adapters.outbound.sqs

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.domain.event.TransferFailedEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import software.amazon.awssdk.services.sqs.model.SendMessageResponse
import java.math.BigDecimal
import java.time.Instant

class TransferDlqSqsProducerTest {

    private val sqsClient: SqsClient = mockk()
    private val awsProperties = AwsProperties().apply {
        sqsDlqQueueName = "transfer-failed"
    }
    private val objectMapper = ObjectMapper().registerModule(JavaTimeModule())

    private lateinit var producer: TransferDlqSqsProducer

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        producer = TransferDlqSqsProducer(sqsClient, awsProperties, objectMapper)
    }

    @Test
    fun `sendToDlq deve buscar url da fila e enviar mensagem para SQS DLQ`() {
        val event = TransferFailedEvent(
            transferId = "550e8400-e29b-41d4-a716-446655440000",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("99000.00"),
            currency = "BRL",
            reason = "Saldo insuficiente",
            failedAt = Instant.parse("2026-09-04T10:35:00Z")
        )

        val queueUrl = "http://localhost:4566/000000000000/transfer-failed"
        every {
            sqsClient.getQueueUrl(match<GetQueueUrlRequest> { it.queueName() == "transfer-failed" })
        } returns GetQueueUrlResponse.builder().queueUrl(queueUrl).build()

        every {
            sqsClient.sendMessage(any<SendMessageRequest>())
        } returns SendMessageResponse.builder().messageId("msg-123").build()

        producer.sendToDlq(event)

        verify(exactly = 1) {
            sqsClient.sendMessage(match<SendMessageRequest> { req ->
                req.queueUrl() == queueUrl &&
                        req.messageBody().contains("\"transferId\":\"550e8400-e29b-41d4-a716-446655440000\"") &&
                        req.messageBody().contains("\"reason\":\"Saldo insuficiente\"") &&
                        req.messageBody().contains("\"amount\":99000.00")
            })
        }
    }

    @Test
    fun `sendToDlq deve utilizar cache em memoria e consultar getQueueUrl apenas uma vez`() {
        val event1 = TransferFailedEvent(
            transferId = "tx-1",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("10.00"),
            currency = "BRL",
            reason = "Erro 1",
            failedAt = Instant.now()
        )
        val event2 = TransferFailedEvent(
            transferId = "tx-2",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("20.00"),
            currency = "BRL",
            reason = "Erro 2",
            failedAt = Instant.now()
        )

        val queueUrl = "http://localhost:4566/000000000000/transfer-failed"
        every {
            sqsClient.getQueueUrl(any<GetQueueUrlRequest>())
        } returns GetQueueUrlResponse.builder().queueUrl(queueUrl).build()

        every {
            sqsClient.sendMessage(any<SendMessageRequest>())
        } returns SendMessageResponse.builder().messageId("msg-123").build()

        producer.sendToDlq(event1)
        producer.sendToDlq(event2)

        // Deve buscar a URL da fila apenas uma vez devido ao cache
        verify(exactly = 1) { sqsClient.getQueueUrl(any<GetQueueUrlRequest>()) }
        verify(exactly = 2) { sqsClient.sendMessage(any<SendMessageRequest>()) }
    }

    @Test
    fun `sendToDlq deve propagar TransientException quando envio ao SQS falhar`() {
        val event = TransferFailedEvent(
            transferId = "tx-err",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("10.00"),
            currency = "BRL",
            reason = "Falha teste",
            failedAt = Instant.now()
        )

        every {
            sqsClient.getQueueUrl(any<GetQueueUrlRequest>())
        } throws RuntimeException("SQS indisponivel")

        org.junit.jupiter.api.assertThrows<com.bank.transfer.domain.exception.TransientException> {
            producer.sendToDlq(event)
        }
    }
}
