package com.bank.transfer.domain.port

import com.bank.transfer.domain.event.TransferFailedEvent

interface TransferDlqProducerPort {
    fun sendToDlq(event: TransferFailedEvent)
}
