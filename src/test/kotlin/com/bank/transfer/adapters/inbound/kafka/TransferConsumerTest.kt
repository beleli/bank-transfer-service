package com.bank.transfer.adapters.inbound.kafka

import com.bank.transfer.adapters.inbound.kafka.avro.TransferRequestEvent
import com.bank.transfer.domain.port.ProcessTransferUseCase
import com.bank.transfer.domain.port.TransferDlqProducerPort
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class TransferConsumerTest {

    private val processTransferUseCase: ProcessTransferUseCase = mockk(relaxed = true)
    private val dlqProducer: TransferDlqProducerPort = mockk(relaxed = true)

    private lateinit var consumer: TransferConsumer

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        consumer = TransferConsumer(processTransferUseCase, dlqProducer)
    }

    @Test
    fun `deve receber evento Avro valido e delegar para o caso de uso`() {
        val event = TransferRequestEvent.newBuilder()
            .setTransferId("550e8400-e29b-41d4-a716-446655440000")
            .setSourceAccountId("acc-123")
            .setDestinationAccountId("acc-456")
            .setAmount("150.75")
            .setCurrency("BRL")
            .setRequestedAt("2025-01-15T10:30:00Z")
            .build()

        consumer.handle(event)

        verify(exactly = 1) {
            processTransferUseCase.processTransfer(match {
                it.transferId == "550e8400-e29b-41d4-a716-446655440000" &&
                it.sourceAccountId == "acc-123" &&
                it.destinationAccountId == "acc-456" &&
                it.amount == BigDecimal("150.75") &&
                it.currency == "BRL"
            })
        }
        verify(exactly = 0) { dlqProducer.sendToDlq(any()) }
    }

    @Test
    fun `deve enviar para DLQ quando ocorrer falha inesperada no processamento do evento Avro`() {
        val event = TransferRequestEvent.newBuilder()
            .setTransferId("550e8400-e29b-41d4-a716-446655440000")
            .setSourceAccountId("acc-123")
            .setDestinationAccountId("acc-456")
            .setAmount("150.75")
            .setCurrency("BRL")
            .setRequestedAt("2025-01-15T10:30:00Z")
            .build()

        every { processTransferUseCase.processTransfer(any()) } throws RuntimeException("Falha de infraestrutura no processamento")

        consumer.handle(event)

        verify(exactly = 1) {
            dlqProducer.sendToDlq(match {
                it.transferId == "550e8400-e29b-41d4-a716-446655440000" &&
                it.sourceAccountId == "acc-123" &&
                it.destinationAccountId == "acc-456" &&
                it.reason.contains("Processing error on Avro event")
            })
        }
    }
}
