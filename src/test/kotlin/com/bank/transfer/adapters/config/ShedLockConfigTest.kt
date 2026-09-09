package com.bank.transfer.adapters.config

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

class ShedLockConfigTest {

    private val dynamoDbClient: DynamoDbClient = mockk()
    private val awsProperties = AwsProperties().apply {
        shedlockTableName = "shedlock-test"
    }

    @Test
    fun `deve criar LockProvider com sucesso para DynamoDB`() {
        val config = ShedLockConfig()
        val lockProvider = config.lockProvider(dynamoDbClient, awsProperties)
        assertNotNull(lockProvider)
    }
}
