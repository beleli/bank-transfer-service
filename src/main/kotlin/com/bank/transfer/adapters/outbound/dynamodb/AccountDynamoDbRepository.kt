package com.bank.transfer.adapters.outbound.dynamodb

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.adapters.outbound.dynamodb.mapper.toDomainAccount
import com.bank.transfer.adapters.outbound.dynamodb.mapper.toItem
import com.bank.transfer.domain.exception.BusinessException
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.AccountStatus
import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.port.AccountRepositoryPort
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.math.BigDecimal

@Repository
class AccountDynamoDbRepository(
    private val dynamoDbClient: DynamoDbClient,
    private val awsProperties: AwsProperties
) : AccountRepositoryPort {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun findById(accountId: String): Account? {
        try {
            val key = mapOf("accountId" to AttributeValue.builder().s(accountId).build())
            val request = GetItemRequest.builder()
                .tableName(awsProperties.accountsTableName)
                .key(key)
                .consistentRead(true)
                .build()

            val response = dynamoDbClient.getItem(request)
            if (!response.hasItem() || response.item().isEmpty()) {
                return null
            }

            return response.item().toDomainAccount(defaultAccountId = accountId)
        } catch (e: SdkClientException) {
            throw TransientException("DynamoDB client error fetching account $accountId: ${e.message}", e)
        } catch (e: DynamoDbException) {
            if (isTransientError(e)) {
                throw TransientException("DynamoDB transient error fetching account $accountId: ${e.message}", e)
            }
            throw e
        }
    }

    override fun save(account: Account) {
        try {
            val request = PutItemRequest.builder()
                .tableName(awsProperties.accountsTableName)
                .item(account.toItem())
                .build()

            dynamoDbClient.putItem(request)
        } catch (e: SdkClientException) {
            throw TransientException("DynamoDB client error saving account ${account.accountId}: ${e.message}", e)
        } catch (e: DynamoDbException) {
            if (isTransientError(e)) {
                throw TransientException("DynamoDB transient error saving account ${account.accountId}: ${e.message}", e)
            }
            throw e
        }
    }

    override fun executeAtomicTransfer(
        sourceAccountId: String,
        destinationAccountId: String,
        amount: BigDecimal,
        transaction: Transaction
    ) {
        val amountStr = amount.toPlainString()

        // 1. Debitar conta de origem com verificação de saldo e status
        val debitUpdate = Update.builder()
            .tableName(awsProperties.accountsTableName)
            .key(mapOf("accountId" to AttributeValue.builder().s(sourceAccountId).build()))
            .updateExpression("SET balance = balance - :amount")
            .conditionExpression("attribute_exists(accountId) AND balance >= :amount AND #status = :active")
            .expressionAttributeNames(mapOf("#status" to "status"))
            .expressionAttributeValues(
                mapOf(
                    ":amount" to AttributeValue.builder().n(amountStr).build(),
                    ":active" to AttributeValue.builder().s(AccountStatus.ACTIVE.name).build()
                )
            )
            .build()

        // 2. Creditar conta de destino com verificação de status ativo
        val creditUpdate = Update.builder()
            .tableName(awsProperties.accountsTableName)
            .key(mapOf("accountId" to AttributeValue.builder().s(destinationAccountId).build()))
            .updateExpression("SET balance = balance + :amount")
            .conditionExpression("attribute_exists(accountId) AND #status = :active")
            .expressionAttributeNames(mapOf("#status" to "status"))
            .expressionAttributeValues(
                mapOf(
                    ":amount" to AttributeValue.builder().n(amountStr).build(),
                    ":active" to AttributeValue.builder().s(AccountStatus.ACTIVE.name).build()
                )
            )
            .build()

        // 3. Registrar transação com garantia de idempotência (attribute_not_exists(transferId))
        val insertTx = Put.builder()
            .tableName(awsProperties.transactionsTableName)
            .item(transaction.toItem())
            .conditionExpression("attribute_not_exists(transferId)")
            .build()

        val transactRequest = TransactWriteItemsRequest.builder()
            .transactItems(
                TransactWriteItem.builder().update(debitUpdate).build(),
                TransactWriteItem.builder().update(creditUpdate).build(),
                TransactWriteItem.builder().put(insertTx).build()
            )
            .build()

        try {
            dynamoDbClient.transactWriteItems(transactRequest)
            logger.info("Transação atômica concluída com sucesso para transferId=${transaction.transferId}")
        } catch (e: TransactionCanceledException) {
            val reasons = e.cancellationReasons()
            logger.warn("Transação cancelada pelo DynamoDB para transferId=${transaction.transferId}. Razões: $reasons")

            // 1. Identificar erros transitórios nos motivos de cancelamento ou no status HTTP da exceção
            val transientReasonCodes = setOf("TransactionConflict", "ThrottlingError", "ProvisionedThroughputExceeded")
            val hasTransientReason = reasons?.any { it.code() in transientReasonCodes } == true
            if (hasTransientReason || isTransientError(e)) {
                throw TransientException("Erro transiente no DynamoDB durante TransactWriteItems para transferId=${transaction.transferId}: ${e.message}", e)
            }

            // 2. Tratar falhas de condição de negócio específicas
            if (reasons != null && reasons.size > 2 && reasons[2].code() == "ConditionalCheckFailed") {
                throw BusinessException.DuplicateTransferException(transaction.transferId)
            }
            if (reasons != null && reasons.isNotEmpty() && reasons[0].code() == "ConditionalCheckFailed") {
                throw BusinessException.InsufficientBalanceException(sourceAccountId, "indisponível", amountStr)
            }
            if (reasons != null && reasons.size > 1 && reasons[1].code() == "ConditionalCheckFailed") {
                throw BusinessException.InactiveAccountException(destinationAccountId)
            }

            // 3. Validação de parâmetros rejeitada pelo DynamoDB
            if (reasons?.any { it.code() == "ValidationError" } == true) {
                throw BusinessException.InvalidAmountException("Erro de validação na transação do DynamoDB: ${e.message}")
            }

            // 4. Cancelamentos não reconhecidos: tratar como erro transiente para permitir retry seguro
            throw TransientException("Cancelamento transacional não reconhecido no DynamoDB para transferId=${transaction.transferId}: ${e.message}", e)
        } catch (e: SdkClientException) {
            throw TransientException("Erro no cliente SDK ao executar TransactWriteItems: ${e.message}", e)
        } catch (e: DynamoDbException) {
            if (isTransientError(e)) {
                throw TransientException("Erro transiente no DynamoDB: ${e.message}", e)
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
