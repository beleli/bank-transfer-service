package com.bank.transfer.adapters.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "aws")
class AwsProperties {
    var region: String = "us-east-1"
    var accessKeyId: String = "test"
    var secretAccessKey: String = "test"
    var dynamodbEndpoint: String? = null
    var sqsEndpoint: String? = null
    var sqsDlqQueueName: String = "transfer-failed"
    var accountsTableName: String = "accounts"
    var transactionsTableName: String = "transactions"
}
