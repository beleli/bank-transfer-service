package com.bank.transfer.adapters.outbound.dynamodb

import com.bank.transfer.adapters.config.AwsProperties
import com.bank.transfer.domain.exception.BusinessException
import com.bank.transfer.domain.exception.TransientException
import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.AccountStatus
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

class AccountDynamoDbRepositoryTest {

    private val dynamoDbClient: DynamoDbClient = mockk()
    private val awsProperties = AwsProperties().apply {
        accountsTableName = "accounts"
        transactionsTableName = "transactions"
    }

    private lateinit var repository: AccountDynamoDbRepository

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        repository = AccountDynamoDbRepository(dynamoDbClient, awsProperties)
    }

    @Test
    fun `findById deve retornar Account mapeada quando conta existir`() {
        val accountId = "acc-123"
        val item = mapOf(
            "accountId" to AttributeValue.builder().s(accountId).build(),
            "balance" to AttributeValue.builder().n("5000.00").build(),
            "currency" to AttributeValue.builder().s("BRL").build(),
            "status" to AttributeValue.builder().s("ACTIVE").build(),
            "customerName" to AttributeValue.builder().s("João Silva").build()
        )
        val response = GetItemResponse.builder().item(item).build()

        every { dynamoDbClient.getItem(any<GetItemRequest>()) } returns response

        val account = repository.findById(accountId)

        assertNotNull(account)
        assertEquals(accountId, account?.accountId)
        assertEquals(BigDecimal("5000.00"), account?.balance)
        assertEquals("BRL", account?.currency)
        assertEquals(AccountStatus.ACTIVE, account?.status)
        assertEquals("João Silva", account?.customerName)

        verify(exactly = 1) {
            dynamoDbClient.getItem(match<GetItemRequest> {
                it.tableName() == "accounts" &&
                        it.key()["accountId"]?.s() == accountId &&
                        it.consistentRead() == true
            })
        }
    }

    @Test
    fun `findById deve retornar null quando conta nao existir no DynamoDB`() {
        val response = GetItemResponse.builder().build()
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } returns response

        val account = repository.findById("acc-inexistente")

        assertNull(account)
    }

    @Test
    fun `findById deve lancar TransientException quando cliente SDK falhar`() {
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } throws SdkClientException.create("Connection timeout")

        val exception = assertThrows<TransientException> {
            repository.findById("acc-123")
        }

        assertTrue(exception.message!!.contains("DynamoDB client error fetching account"))
    }

    @Test
    fun `findById deve lancar TransientException para erros 500 do DynamoDB`() {
        val dynamoEx = DynamoDbException.builder()
            .statusCode(500)
            .message("Internal server error")
            .build()
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } throws dynamoEx

        assertThrows<TransientException> {
            repository.findById("acc-123")
        }
    }

    @Test
    fun `findById deve propagar DynamoDbException quando erro for nao transiente (400)`() {
        val dynamoEx = ResourceNotFoundException.builder()
            .statusCode(400)
            .message("Cannot find table")
            .build()
        every { dynamoDbClient.getItem(any<GetItemRequest>()) } throws dynamoEx

        assertThrows<ResourceNotFoundException> {
            repository.findById("acc-123")
        }
    }

    @Test
    fun `save deve persistir conta com sucesso chamando putItem`() {
        val account = Account("acc-123", BigDecimal("100.00"), "BRL", AccountStatus.ACTIVE, "Maria")
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } returns PutItemResponse.builder().build()

        repository.save(account)

        verify(exactly = 1) {
            dynamoDbClient.putItem(match<PutItemRequest> {
                it.tableName() == "accounts" &&
                        it.item()["accountId"]?.s() == "acc-123" &&
                        it.item()["balance"]?.n() == "100.00"
            })
        }
    }

    @Test
    fun `save deve lancar TransientException quando ocorrer SdkClientException`() {
        val account = Account("acc-123", BigDecimal("100.00"), "BRL", AccountStatus.ACTIVE, "Maria")
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws SdkClientException.create("Network down")

        assertThrows<TransientException> {
            repository.save(account)
        }
    }

    @Test
    fun `save deve lancar TransientException quando ocorrer ProvisionedThroughputExceededException`() {
        val account = Account("acc-123", BigDecimal("100.00"), "BRL", AccountStatus.ACTIVE, "Maria")
        val throughputEx = ProvisionedThroughputExceededException.builder()
            .statusCode(400)
            .message("Throughput exceeded")
            .build()
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws throughputEx

        assertThrows<TransientException> {
            repository.save(account)
        }
    }

    @Test
    fun `executeAtomicTransfer deve executar debito, credito e insercao via transactWriteItems`() {
        val tx = Transaction(
            transferId = "tx-123",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now(),
            completedAt = Instant.now()
        )
        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } returns TransactWriteItemsResponse.builder().build()

        repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)

        verify(exactly = 1) {
            dynamoDbClient.transactWriteItems(match<TransactWriteItemsRequest> { req ->
                req.transactItems().size == 3 &&
                        req.transactItems()[0].update().tableName() == "accounts" &&
                        req.transactItems()[1].update().tableName() == "accounts" &&
                        req.transactItems()[2].put().tableName() == "transactions"
            })
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar DuplicateTransferException quando terceira condicao falhar`() {
        val tx = Transaction(
            transferId = "tx-dup",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val reasons = listOf(
            CancellationReason.builder().code("None").build(),
            CancellationReason.builder().code("None").build(),
            CancellationReason.builder().code("ConditionalCheckFailed").message("Item already exists").build()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(reasons)
            .message("Transaction cancelled")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        val ex = assertThrows<BusinessException.DuplicateTransferException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
        assertTrue(ex.message!!.contains("tx-dup"))
    }

    @Test
    fun `executeAtomicTransfer deve lancar InsufficientBalanceException quando primeira condicao falhar`() {
        val tx = Transaction(
            transferId = "tx-saldo",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("5000.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val reasons = listOf(
            CancellationReason.builder().code("ConditionalCheckFailed").message("Insufficient balance").build(),
            CancellationReason.builder().code("None").build()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(reasons)
            .message("Transaction cancelled")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        val ex = assertThrows<BusinessException.InsufficientBalanceException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("5000.00"), tx)
        }
        assertTrue(ex.message!!.contains("acc-1"))
    }

    @Test
    fun `executeAtomicTransfer deve lancar InactiveAccountException quando segunda condicao falhar`() {
        val tx = Transaction(
            transferId = "tx-inativo",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-inativa",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val reasons = listOf(
            CancellationReason.builder().code("None").build(),
            CancellationReason.builder().code("ConditionalCheckFailed").message("Dest inactive").build()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(reasons)
            .message("Transaction cancelled")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        val ex = assertThrows<BusinessException.InactiveAccountException> {
            repository.executeAtomicTransfer("acc-1", "acc-inativa", BigDecimal("50.00"), tx)
        }
        assertTrue(ex.message!!.contains("acc-inativa"))
    }

    @Test
    fun `executeAtomicTransfer deve lancar InvalidAmountException para outros motivos de cancelamento`() {
        val tx = Transaction(
            transferId = "tx-outro",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val reasons = listOf(
            CancellationReason.builder().code("ValidationError").message("Validation error").build()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(reasons)
            .message("Transaction cancelled")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        assertThrows<BusinessException.InvalidAmountException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar TransientException para SdkClientException`() {
        val tx = Transaction(
            transferId = "tx-sdk",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws SdkClientException.create("Socket timeout")

        assertThrows<TransientException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar TransientException para DynamoDbException com status 429`() {
        val tx = Transaction(
            transferId = "tx-throttle",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val dynamoEx = DynamoDbException.builder()
            .statusCode(429)
            .message("Too Many Requests")
            .build()
        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws dynamoEx

        assertThrows<TransientException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `save deve propagar DynamoDbException quando erro for nao transiente (400)`() {
        val account = Account("acc-123", BigDecimal("100.00"), "BRL", AccountStatus.ACTIVE, "Maria")
        val dynamoEx = ResourceNotFoundException.builder().statusCode(400).message("Table missing").build()
        every { dynamoDbClient.putItem(any<PutItemRequest>()) } throws dynamoEx


        assertThrows<ResourceNotFoundException> {
            repository.save(account)
        }
    }

    @Test
    fun `executeAtomicTransfer deve propagar DynamoDbException quando erro for nao transiente (400)`() {
        val tx = Transaction(
            transferId = "tx-400",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val dynamoEx = ResourceNotFoundException.builder().statusCode(400).message("Table missing").build()
        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws dynamoEx

        assertThrows<ResourceNotFoundException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar TransientException quando motivo for TransactionConflict`() {
        val tx = Transaction(
            transferId = "tx-conflict",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val reasons = listOf(
            CancellationReason.builder().code("TransactionConflict").message("Item modified concurrently").build(),
            CancellationReason.builder().code("None").build(),
            CancellationReason.builder().code("None").build()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(reasons)
            .message("Transaction cancelled")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        assertThrows<TransientException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar TransientException quando motivo for ThrottlingError`() {
        val tx = Transaction(
            transferId = "tx-throttling",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val reasons = listOf(
            CancellationReason.builder().code("ThrottlingError").message("Rate exceeded").build()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(reasons)
            .message("Transaction cancelled")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        assertThrows<TransientException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar TransientException para cancelamento transacional com motivo desconhecido`() {
        val tx = Transaction(
            transferId = "tx-unknown-cancel",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val cancelEx = TransactionCanceledException.builder()
            .cancellationReasons(emptyList())
            .message("Transaction cancelled without reasons")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        assertThrows<TransientException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }

    @Test
    fun `executeAtomicTransfer deve lancar TransientException quando TransactionCanceledException tiver status HTTP 429`() {
        val tx = Transaction(
            transferId = "tx-status-429",
            sourceAccountId = "acc-1",
            destinationAccountId = "acc-2",
            amount = BigDecimal("50.00"),
            currency = "BRL",
            status = TransactionStatus.COMPLETED,
            createdAt = Instant.now()
        )
        val cancelEx = TransactionCanceledException.builder()
            .statusCode(429)
            .message("Too Many Requests")
            .build()

        every { dynamoDbClient.transactWriteItems(any<TransactWriteItemsRequest>()) } throws cancelEx

        assertThrows<TransientException> {
            repository.executeAtomicTransfer("acc-1", "acc-2", BigDecimal("50.00"), tx)
        }
    }
}

