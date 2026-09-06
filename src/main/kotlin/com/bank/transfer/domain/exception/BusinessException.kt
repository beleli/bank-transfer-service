package com.bank.transfer.domain.exception

sealed class BusinessException(message: String) : RuntimeException(message) {
    class AccountNotFoundException(accountId: String) :
        BusinessException("Account not found: $accountId")

    class InactiveAccountException(accountId: String) :
        BusinessException("Destination account is not active: $accountId")

    class InsufficientBalanceException(accountId: String, currentBalance: String, requestedAmount: String) :
        BusinessException("Account $accountId has insufficient balance ($currentBalance) for transfer of $requestedAmount")

    class InvalidCurrencyException(currency: String) :
        BusinessException("Unsupported currency: $currency. Only BRL is accepted.")

    class InvalidAmountException(message: String) :
        BusinessException(message)

    class DuplicateTransferException(transferId: String) :
        BusinessException("Transfer already processed (idempotency violation): $transferId")
}
