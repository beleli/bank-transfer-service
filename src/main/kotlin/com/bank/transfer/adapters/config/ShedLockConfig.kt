package com.bank.transfer.adapters.config

import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.dynamodb2.DynamoDBLockProvider
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

@Configuration
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
class ShedLockConfig {

    @Bean
    fun lockProvider(dynamoDbClient: DynamoDbClient, awsProperties: AwsProperties): LockProvider {
        return DynamoDBLockProvider(dynamoDbClient, awsProperties.shedlockTableName)
    }
}
