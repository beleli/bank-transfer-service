package com.bank.transfer.domain.port

import com.bank.transfer.domain.model.TransferRequest

interface ProcessTransferUseCase {
    fun processTransfer(request: TransferRequest)
}
