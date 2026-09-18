package com.bank.transfer

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import org.junit.jupiter.api.Assertions.assertTrue

class LogbackJsonConfigTest {

    @Test
    fun `deve validar saida em JSON com transferId no logback-spring`() {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        context.reset()
        val configurator = JoranConfigurator()
        configurator.context = context
        val configStream = javaClass.classLoader.getResourceAsStream("logback-spring.xml")
            ?: throw IllegalStateException("logback-spring.xml not found")
        configurator.doConfigure(configStream)

        val originalOut = System.out
        val outContent = ByteArrayOutputStream()
        try {
            System.setOut(PrintStream(outContent))

            MDC.put("transferId", "test-transfer-id-12345")
            MDC.put("correlationId", "test-corr-id")
            MDC.put("sourceAccountId", "acc-src")
            MDC.put("destinationAccountId", "acc-dst")

            val logger = LoggerFactory.getLogger(LogbackJsonConfigTest::class.java)
            logger.info("Testando log estruturado em JSON")
        } finally {
            MDC.clear()
            System.setOut(originalOut)
        }

        val logOutput = outContent.toString()
        println("Captured Log Output:\n$logOutput")

        val objectMapper = com.fasterxml.jackson.databind.ObjectMapper()
        val jsonNode = objectMapper.readTree(logOutput.trim())

        assertTrue(jsonNode.has("service"), "Deve conter campo customizado service")
        org.junit.jupiter.api.Assertions.assertEquals("bank-transfer-service", jsonNode.get("service").asText())
        assertTrue(jsonNode.has("transferId"), "Deve conter transferId no MDC")
        org.junit.jupiter.api.Assertions.assertEquals("test-transfer-id-12345", jsonNode.get("transferId").asText())
        org.junit.jupiter.api.Assertions.assertEquals("acc-src", jsonNode.get("sourceAccountId").asText())
        org.junit.jupiter.api.Assertions.assertEquals("acc-dst", jsonNode.get("destinationAccountId").asText())
        org.junit.jupiter.api.Assertions.assertEquals("Testando log estruturado em JSON", jsonNode.get("message").asText())
    }

    @Test
    fun `deve validar que TransferConsumer emite logs no formato JSON com transferId quando executado`() {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        context.reset()
        val configurator = JoranConfigurator()
        configurator.context = context
        val configStream = javaClass.classLoader.getResourceAsStream("logback-spring.xml")
            ?: throw IllegalStateException("logback-spring.xml not found")
        configurator.doConfigure(configStream)

        val processUseCase = io.mockk.mockk<com.bank.transfer.domain.port.ProcessTransferUseCase>(relaxed = true)
        val dlqProducer = io.mockk.mockk<com.bank.transfer.domain.port.TransferDlqProducerPort>(relaxed = true)
        val consumer = com.bank.transfer.adapters.inbound.kafka.TransferConsumer(processUseCase, dlqProducer)

        val event = com.bank.transfer.adapters.inbound.kafka.avro.TransferRequestEvent.newBuilder()
            .setTransferId("e89f8164-f655-4a60-93a8-e160a3fa7167")
            .setSourceAccountId("acc-123")
            .setDestinationAccountId("acc-456")
            .setAmount(250.00)
            .setCurrency("BRL")
            .setRequestedAt("2026-09-09T10:00:00Z")
            .build()

        val originalOut = System.out
        val outContent = ByteArrayOutputStream()
        try {
            System.setOut(PrintStream(outContent))
            consumer.handle(event)
        } finally {
            System.setOut(originalOut)
        }

        val logOutput = outContent.toString()
        println("TransferConsumer Log Output:\n$logOutput")

        val objectMapper = com.fasterxml.jackson.databind.ObjectMapper()
        val jsonNode = objectMapper.readTree(logOutput.trim())

        org.junit.jupiter.api.Assertions.assertEquals("e89f8164-f655-4a60-93a8-e160a3fa7167", jsonNode.get("transferId").asText())
        org.junit.jupiter.api.Assertions.assertEquals("acc-123", jsonNode.get("sourceAccountId").asText())
        org.junit.jupiter.api.Assertions.assertEquals("acc-456", jsonNode.get("destinationAccountId").asText())
        org.junit.jupiter.api.Assertions.assertEquals("bank-transfer-service", jsonNode.get("service").asText())
        assertTrue(jsonNode.get("message").asText().contains("Mensagem Avro recebida"))
    }
}
