package com.bank.transfer.adapters.outbound.dynamodb

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.domain.exception.BusinessException
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
    fun `save deve persistir transacao com condicao attribute_not_exists para garantir idempotencia`() {
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
                        it.conditionExpression() == "attribute_not_exists(transferId)"
            })
        }
    }

    @Test
    fun `save deve persistir transacao com status FAILED com condicao attribute_not_exists para idempotencia`() {
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
                        it.conditionExpression() == "attribute_not_exists(transferId)"
            })
        }
    }

    @Test
    fun `save deve lancar DuplicateTransferException quando ConditionalCheckFailedException ocorrer`() {
        val tx = Transaction(
            transferId = "tx-already-exists",
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

        assertThrows<BusinessException.DuplicateTransferException> {
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

    @Test
    fun `findUnpublished deve executar scan com filtro de published false e status COMPLETED`() {
        val item = mapOf(
            "transferId" to AttributeValue.builder().s("tx-unpub").build(),
            "sourceAccountId" to AttributeValue.builder().s("acc-1").build(),
            "destinationAccountId" to AttributeValue.builder().s("acc-2").build(),
            "amount" to AttributeValue.builder().n("100.00").build(),
            "currency" to AttributeValue.builder().s("BRL").build(),
            "status" to AttributeValue.builder().s("COMPLETED").build(),
            "createdAt" to AttributeValue.builder().s(Instant.now().toString()).build(),
            "published" to AttributeValue.builder().bool(false).build()
        )
        val scanResponse = ScanResponse.builder().items(listOf(item)).build()
        every { dynamoDbClient.scan(any<ScanRequest>()) } returns scanResponse

        val result = repository.findUnpublished(limit = 10)

        assertEquals(1, result.size)
        assertEquals("tx-unpub", result[0].transferId)
        assertFalse(result[0].published)

        verify(exactly = 1) {
            dynamoDbClient.scan(match<ScanRequest> {
                it.tableName() == "transactions" &&
                it.limit() == 10 &&
                it.filterExpression().contains("published = :published") &&
                it.expressionAttributeValues()[":published"]?.bool() == false
            })
        }
    }

    @Test
    fun `findUnpublished deve lancar TransientException quando DynamoDbClient falhar com erro transiente`() {
        val dynamoEx = DynamoDbException.builder().statusCode(503).message("Unavailable").build()
        every { dynamoDbClient.scan(any<ScanRequest>()) } throws dynamoEx

        assertThrows<TransientException> {
            repository.findUnpublished(limit = 10)
        }
    }
}

