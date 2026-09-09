package com.bank.transfer.adapters.outbound.sqs

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.domain.event.TransferFailedEvent
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.port.TransferDlqProducerPort
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import java.util.concurrent.ConcurrentHashMap

@Component
class TransferDlqSqsProducer(
    private val sqsClient: SqsClient,
    private val awsProperties: AwsProperties,
    private val objectMapper: ObjectMapper
) : TransferDlqProducerPort {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private val queueUrlCache = ConcurrentHashMap<String, String>()

    override fun sendToDlq(event: TransferFailedEvent) {
        try {
            val queueUrl = getQueueUrl(awsProperties.sqsDlqQueueName)
            val messageBody = objectMapper.writeValueAsString(event)

            val messageAttributes = mapOf(
                "transferId" to software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder().dataType("String").stringValue(event.transferId).build(),
                "reason" to software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder().dataType("String").stringValue(event.reason.take(256)).build(),
                "timestamp" to software.amazon.awssdk.services.sqs.model.MessageAttributeValue.builder().dataType("String").stringValue(event.failedAt.toString()).build()
            )

            val sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(messageAttributes)
                .build()

            sqsClient.sendMessage(sendMsgRequest)
            logger.info("Transferência rejeitada enviada para SQS DLQ: queue=${awsProperties.sqsDlqQueueName} transferId=${event.transferId} reason=${event.reason}")
        } catch (e: Exception) {
            logger.error("Falha crítica ao enviar mensagem para SQS DLQ transferId=${event.transferId}: ${e.message}", e)
            throw TransientException("Falha ao enviar mensagem para SQS DLQ transferId=${event.transferId}: ${e.message}", e)
        }
    }

    private fun getQueueUrl(queueName: String): String {
        return queueUrlCache.computeIfAbsent(queueName) { name ->
            val response = sqsClient.getQueueUrl(
                GetQueueUrlRequest.builder().queueName(name).build()
            )
            response.queueUrl()
        }
    }
}
