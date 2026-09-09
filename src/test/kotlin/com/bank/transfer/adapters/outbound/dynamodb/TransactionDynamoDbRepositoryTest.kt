package com.bank.transfer.adapters.outbound.dynamodb

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.model.Transaction
import com.bank.transfer.domain.model.TransactionStatus
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.math.BigDecimal
import java.time.Instant

class TransactionDynamoDbRepositoryTest {

    private val dynamoDbClient: DynamoDbClient = mockk()
    private val awsProperties = AwsProperties().apply {
        accountsTableName = "accounts"
        transactionsTableName = "transactions"
    }

    private lateinit var repository: TransactionDynamoDbRepository

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        repository = TransactionDynamoDbRepository(dynamoDbClient, awsProperties)
    }

    @Test
    fun `findById deve retornar Transaction mapeada quando transacao existir`() {
        val transferId = "tx-123"
        val nowStr = Instant.now().toString()
        val item = mapOf(
            "transferId" to AttributeValue.builder().s(transferId).build(),
            "sourceAccountId" to AttributeValue.builder().s("acc-1").build(),
            "destinationAccountId" to AttributeValue.builder().s("acc-2").build(),
            "amount" to AttributeValue.builder().n("150.00").build(),
            "currency" to AttributeValue.builder().s("BRL").build(),
            "status" to AttributeValue.builder().s("COMPLETED").build(),
            "createdAt" to AttributeValue.builder().s(nowStr).build(),
            "completedAt" to AttributeValue.builder().s(nowStr).build()
        )
        val response = GetItemResponse.builder().item(item).build()

        every { dynamoDbClient.getItem(any<GetItemRequest>()) } returns response

        val transaction = repository.findById(transferId)

        assertNotNull(transaction)
        assertEquals(transferId, transaction?.transferId)
        assertEquals("acc-1", transaction?.sourceAccountId)
        assertEquals("acc-2", transaction?.destinationAccountId)
        assertEquals(BigDecimal("150.00"), transaction?.amount)
        assertEquals(TransactionStatus.COMPLETED, transaction?.status)

        verify(exactly = 1) {
            dynamoDbClient.getItem(match<GetItemRequest> {
                it.tableName() == "transactions" &&
                        it.key()["transferId"]?.s() == transferId &&
                        it.consistentRead() == true
            })
        }
    }

    @Test
    fun `findById deve retornar null quando transacao nao existir`() {
        val response = GetItemResponse.builder().build()
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } returns response

        val transaction = repository.findById("tx-inexistente")

        assertNull(transaction)
    }

    @Test
    fun `findById deve lancar TransientException para SdkClientException`() {
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } throws SdkClientException.create("Read timeout")

        assertThrows<TransientException> {
            repository.findById("tx-123")
        }
    }

    @Test
    fun `findById deve lancar TransientException para DynamoDbException transiente (status 503)`() {
        val dynamoEx = DynamoDbException.builder()
            .statusCode(503)
            .message("Service Unavailable")
            .build()
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } throws dynamoEx

        assertThrows<TransientException> {
            repository.findById("tx-123")
        }
    }

    @Test
    fun `findById deve propagar DynamoDbException quando erro for nao transiente (400)`() {
        val dynamoEx = ResourceNotFoundException.builder()
            .statusCode(400)
            .message("Table not found")
            .build()
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } throws dynamoEx

        assertThrows<ResourceNotFoundException> {
            repository.findById("tx-123")
        }
    }

    @Test
    fun `save deve persistir transacao com status COMPLETED sem condicao restritiva`() {
        val tx = Transaction(
            transferId = "tx-comp",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            completedAt = Instant.now()
        )
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } returns PutItemResponse.builder().build()

        repository.save(tx)

        verify(exactly = 1) {
            dynamoDbClient.putItem(match<PutItemRequest> {
                it.tableName() == "transactions" &&
                        it.item()["transferId"]?.s() == "tx-comp" &&
                        it.conditionExpression() == null
            })
        }
    }

    @Test
    fun `save deve persistir transacao com status FAILED com condicao para nao sobrescrever COMPLETED`() {
        val tx = Transaction(
            transferId = "tx-fail",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.FAILED,
            rejectionReason = "Saldo insuficiente",
            createdAt = Instant.now()
        )
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } returns PutItemResponse.builder().build()

        repository.save(tx)

        verify(exactly = 1) {
            dynamoDbClient.putItem(match<PutItemRequest> {
                it.tableName() == "transactions" &&
                        it.item()["transferId"]?.s() == "tx-fail" &&
                        it.conditionExpression() == "attribute_not_exists(transferId) OR #status <> :completed" &&
                        it.expressionAttributeNames()["#status"] == "status" &&
                        it.expressionAttributeValues()[":completed"]?.s() == "COMPLETED"
            })
        }
    }

    @Test
    fun `save deve ignorar silenciosamente quando ConditionalCheckFailedException ocorrer para status FAILED`() {
        val tx = Transaction(
            transferId = "tx-already-completed",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.FAILED,
            rejectionReason = "Erro posterior",
            createdAt = Instant.now()
        )
        val condEx = ConditionalCheckFailedException.builder().message("Condition check failed").build()
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws condEx

        // Não deve propagar exceção
        assertDoesNotThrow {
            repository.save(tx)
        }
    }

    @Test
    fun `save deve lancar TransientException para SdkClientException`() {
        val tx = Transaction(
            transferId = "tx-err",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws SdkClientException.create("Network error")

        assertThrows<TransientException> {
            repository.save(tx)
        }
    }

    @Test
    fun `save deve lancar TransientException para DynamoDbException com status 500`() {
        val tx = Transaction(
            transferId = "tx-err",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val dynamoEx = DynamoDbException.builder().statusCode(500).message("Server error").build()
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws dynamoEx

        assertThrows<TransientException> {
            repository.save(tx)
        }
    }

    @Test
    fun `save deve propagar DynamoDbException quando erro for nao transiente (400)`() {
        val tx = Transaction(
            transferId = "tx-err",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("100.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val dynamoEx = ResourceNotFoundException.builder().statusCode(400).message("Table missing").build()
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws dynamoEx

        assertThrows<ResourceNotFoundException> {
            repository.save(tx)
        }
    }

    @Test
    fun `markAsPublished deve executar updateItem com published true`() {
        val transferId = "tx-published-123"
        val response = UpdateItemResponse.builder().build()
        every { dynamoDbClient.updateItem(any<UpdateItemRequest>()) } returns response

        repository.markAsPublished(transferId)

        verify(exactly = 1) {
            dynamoDbClient.updateItem(match<UpdateItemRequest> {
                it.tableName() == "transactions" &&
                        it.key()["transferId"]?.s() == transferId &&
                        it.updateExpression().contains("published = :published") &&
                        it.expressionAttributeValues()[":published"]?.bool() == true
            })
        }
    }

    @Test
    fun `markAsPublished deve lancar TransientException quando DynamoDbClient falhar com erro transiente`() {
        val transferId = "tx-published-err"
        val dynamoEx = DynamoDbException.builder().statusCode(500).message("Internal error").build()
        every { dynamoDbClient.updateItem(any<UpdateItemRequest>()) } throws dynamoEx

        assertThrows<TransientException> {
            repository.markAsPublished(transferId)
        }
    }
}

