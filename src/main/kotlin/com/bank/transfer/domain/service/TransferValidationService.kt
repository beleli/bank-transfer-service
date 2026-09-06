package com.bank.transfer.domain.service

import com.bank.transfer.domain.exception.BusinessException
import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.AccountStatus
import com.bank.transfer.domain.model.TransferRequest
import java.math.BigDecimal

class TransferValidationService {

    fun validate(
        request: TransferRequest,
        sourceAccount: Account?,
        destinationAccount: Account?
    ) {
        // 1. Validar moeda
        if (!request.currency.equals("BRL", ignoreCase = true)) {
            throw BusinessException.InvalidCurrencyException(request.currency)
        }

        // 2. Validar valor positivo
        if (request.amount <= BigDecimal.ZERO) {
            throw BusinessException.InvalidAmountException("Transfer amount must be positive, received: ${request.amount}")
        }

        // 3. Validar contas distintas
        if (request.sourceAccountId == request.destinationAccountId) {
            throw BusinessException.InvalidAmountException("Source and destination accounts must be different: ${request.sourceAccountId}")
        }

        // 4. Validar conta de origem existente
        if (sourceAccount == null) {
            throw BusinessException.AccountNotFoundException(request.sourceAccountId)
        }

        // 5. Validar conta de origem ativa
        if (sourceAccount.status != AccountStatus.ACTIVE) {
            throw BusinessException.InactiveAccountException(request.sourceAccountId)
        }

        // 6. Validar saldo suficiente
        if (sourceAccount.balance < request.amount) {
            throw BusinessException.InsufficientBalanceException(
                accountId = request.sourceAccountId,
                currentBalance = sourceAccount.balance.toPlainString(),
                requestedAmount = request.amount.toPlainString()
            )
        }

        // 7. Validar conta de destino existente
        if (destinationAccount == null) {
            throw BusinessException.AccountNotFoundException(request.destinationAccountId)
        }

        // 8. Validar conta de destino ativa
        if (destinationAccount.status != AccountStatus.ACTIVE) {
            throw BusinessException.InactiveAccountException(request.destinationAccountId)
        }
    }
}
