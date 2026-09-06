package com.bank.transfer.domain.port

import com.bank.transfer.domain.event.TransferCompletedEvent

interface TransferCompletedProducerPort {
    fun publish(event: TransferCompletedEvent)
}
