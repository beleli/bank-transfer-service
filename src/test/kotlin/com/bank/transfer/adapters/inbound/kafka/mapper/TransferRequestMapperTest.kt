package com.bank.transfer.adapters.inbound.kafka.mapper

import com.bank.transfer.adapters.inbound.kafka.dto.TransferRequestDto
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.Instant

class TransferRequestMapperTest {

    @Test
    fun `deve mapear dto para modelo de dominio com sucesso`() {
        val now = Instant.now()
        val dto = TransferRequestDto(
            transferId = "tx-100",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("250.00"),
            currency = "BRL",
            requestedAt = now
        )

        val domain = dto.toDomain()

        assertEquals("tx-100", domain.transferId)
        assertEquals("acc-1", domain.sourceAccountId)
        assertEquals("acc-2", domain.destinationAccountId)
        assertEquals(BigDecimal("250.00"), domain.amount)
        assertEquals("BRL", domain.currency)
        assertEquals(now, domain.requestedAt)
    }

    @Test
    fun `deve lancar excecao quando campo obrigatorio for nulo`() {
        val dto = TransferRequestDto(
            transferId = null,
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("250.00"),
            currency = "BRL"
        )

        assertThrows<IllegalArgumentException> {
            dto.toDomain()
        }
    }

    @Test
    fun `deve mapear TransferRequestEvent do Avro para modelo de dominio com sucesso`() {
        val event = com.bank.transfer.adapters.inbound.kafka.avro.TransferRequestEvent.newBuilder()
            .setTransferId("tx-avro-1")
            .setSourceAccountId("acc-src-1")
            .setDestinationAccountId("acc-dst-2")
            .setAmount(150.75)
            .setCurrency("BRL")
            .setRequestedAt("2025-01-15T10:30:00Z")
            .build()

        val domain = event.toDomain()

        assertEquals("tx-avro-1", domain.transferId)
        assertEquals("acc-src-1", domain.sourceAccountId)
        assertEquals("acc-dst-2", domain.destinationAccountId)
        assertEquals(BigDecimal("150.75"), domain.amount)
        assertEquals("BRL", domain.currency)
        assertEquals(Instant.parse("2025-01-15T10:30:00Z"), domain.requestedAt)
    }

    @Test
    fun `deve mapear TransferRequestEvent com requestedAt em branco gerando data atual`() {
        val event = com.bank.transfer.adapters.inbound.kafka.avro.TransferRequestEvent.newBuilder()
            .setTransferId("tx-avro-2")
            .setSourceAccountId("acc-src-1")
            .setDestinationAccountId("acc-dst-2")
            .setAmount(100.00)
            .setCurrency("BRL")
            .setRequestedAt("")
            .build()

        val domain = event.toDomain()

        assertEquals("tx-avro-2", domain.transferId)
        assertEquals(BigDecimal("100.00"), domain.amount)
        org.junit.jupiter.api.Assertions.assertNotNull(domain.requestedAt)
    }
}
