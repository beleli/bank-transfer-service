package com.bank.transfer.adapters.outbound.dynamodb

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.adapters.outbound.dynamodb.mapper.toDomainTransactionRecord
import com.bank.transfer.adapters.outbound.dynamodb.mapper.toItem
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.model.TransactionStatus
import com.bank.transfer.domain.port.TransactionRepositoryPort
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*

@Repository
class TransactionDynamoDbRepository(
    private val dynamoDbClient: DynamoDbClient,
    private val awsProperties: AwsProperties
) : TransactionRepositoryPort {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun findById(transferId: String): Transaction? {
        try {
            val key = mapOf("transferId" to AttributeValue.builder().s(transferId).build())
            val request = GetItemRequest.builder()
                .tableName(awsProperties.transactionsTableName)
                .key(key)
                .consistentRead(true)
                .build()

            val response = dynamoDbClient.getItem(request)
            if (!response.hasItem() || response.item().isEmpty()) {
                return null
            }

            return response.item().toDomainTransactionRecord(defaultTransferId = transferId)
        } catch (e: SdkClientException) {
            throw TransientException("DynamoDB client error fetching transaction $transferId: ${e.message}", e)
        } catch (e: DynamoDbException) {
            if (isTransientError(e)) {
                throw TransientException("DynamoDB transient error fetching transaction $transferId: ${e.message}", e)
            }
            throw e
        }
    }

    override fun save(record: Transaction) {
        try {
            val item = record.toItem()

            val requestBuilder = PutItemRequest.builder()
                .tableName(awsProperties.transactionsTableName)
                .item(item)

            // Se for status FAILED, previne sobrescrever uma transação já concluída com sucesso (COMPLETED)
            if (record.status == TransactionStatus.FAILED) {
                requestBuilder
                    .conditionExpression("attribute_not_exists(transferId) OR #status <> :completed")
                    .expressionAttributeNames(mapOf("#status" to "status"))
                    .expressionAttributeValues(
                        mapOf(":completed" to AttributeValue.builder().s(TransactionStatus.COMPLETED.name).build())
                    )
            }

            dynamoDbClient.putItem(requestBuilder.build())
            logger.info("Registro de transação salvo para transferId=${record.transferId} status=${record.status}")
        } catch (_: ConditionalCheckFailedException) {
            logger.warn("Transação transferId=${record.transferId} já se encontra COMPLETED. Sobrescrita para FAILED evitada.")
        } catch (e: SdkClientException) {
            throw TransientException("DynamoDB client error saving transaction ${record.transferId}: ${e.message}", e)
        } catch (e: DynamoDbException) {
            if (isTransientError(e)) {
                throw TransientException("DynamoDB transient error saving transaction ${record.transferId}: ${e.message}", e)
            }
            throw e
        }
    }

    override fun markAsPublished(transferId: String) {
        try {
            val key = mapOf("transferId" to AttributeValue.builder().s(transferId).build())
            val request = UpdateItemRequest.builder()
                .tableName(awsProperties.transactionsTableName)
                .key(key)
                .updateExpression("SET published = :published, publishedAt = :publishedAt")
                .expressionAttributeValues(
                    mapOf(
                        ":published" to AttributeValue.builder().bool(true).build(),
                        ":publishedAt" to AttributeValue.builder().s(java.time.Instant.now().toString()).build()
                    )
                )
                .build()

            dynamoDbClient.updateItem(request)
            logger.info("Transação transferId=$transferId marcada como publicada no Kafka")
        } catch (e: SdkClientException) {
            throw TransientException("DynamoDB client error marking transaction $transferId as published: ${e.message}", e)
        } catch (e: DynamoDbException) {
            if (isTransientError(e)) {
                throw TransientException("DynamoDB transient error marking transaction $transferId as published: ${e.message}", e)
            }
            throw e
        }
    }

    private fun isTransientError(e: DynamoDbException): Boolean {
        val statusCode = e.statusCode()
        return statusCode == 429 || statusCode >= 500 ||
                e is ProvisionedThroughputExceededException ||
                e is RequestLimitExceededException ||
                e is InternalServerErrorException
    }
}
