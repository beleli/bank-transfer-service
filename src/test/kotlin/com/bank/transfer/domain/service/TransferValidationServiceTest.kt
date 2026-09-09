package com.bank.transfer.domain.service

import com.bank.transfer.domain.exception.BusinessException
import com.bank.transfer.domain.model.Account
import com.bank.transfer.domain.model.AccountStatus
import com.bank.transfer.domain.model.TransferRequest
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal

class TransferValidationServiceTest {

    private val validationService = TransferValidationService()

    private val validSourceAccount = Account(
        accountId = "acc-123",
        balance = BigDecimal("5000.00"),
        currency = "BRL",
        status = AccountStatus.ACTIVE,
        customerName = "João Silva"
    )

    private val validDestinationAccount = Account(
        accountId = "acc-456",
        balance = BigDecimal("1200.50"),
        currency = "BRL",
        status = AccountStatus.ACTIVE,
        customerName = "Maria Santos"
    )

    @Test
    fun `deve validar com sucesso quando todos os dados estiverem corretos`() {
        val request = TransferRequest(
            transferId = "tx-1",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("150.75"),
            currency = "BRL"
        )

        assertDoesNotThrow {
            validationService.validate(request, validSourceAccount, validDestinationAccount)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando moeda nao for BRL`() {
        val request = TransferRequest(
            transferId = "tx-2",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("100.00"),
            currency = "USD"
        )

        assertThrows<BusinessException.InvalidCurrencyException> {
            validationService.validate(request, validSourceAccount, validDestinationAccount)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando valor for zero ou negativo`() {
        val requestZero = TransferRequest(
            transferId = "tx-3",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("0.00"),
            currency = "BRL"
        )

        assertThrows<BusinessException.InvalidAmountException> {
            validationService.validate(requestZero, validSourceAccount, validDestinationAccount)
        }

        val requestNegative = TransferRequest(
            transferId = "tx-4",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("-50.00"),
            currency = "BRL"
        )

        assertThrows<BusinessException.InvalidAmountException> {
            validationService.validate(requestNegative, validSourceAccount, validDestinationAccount)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando conta de origem for igual a de destino`() {
        val request = TransferRequest(
            transferId = "tx-5",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-123",
            amount = BigDecimal("100.00"),
            currency = "BRL"
        )

        assertThrows<BusinessException.InvalidAmountException> {
            validationService.validate(request, validSourceAccount, validDestinationAccount)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando conta de origem nao existir`() {
        val request = TransferRequest(
            transferId = "tx-6",
            sourceAccountId = "acc-999",
            destinationAccountId = "acc-456",
            amount = BigDecimal("100.00"),
            currency = "BRL"
        )

        assertThrows<BusinessException.AccountNotFoundException> {
            validationService.validate(request, null, validDestinationAccount)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando conta de origem tiver saldo insuficiente`() {
        val request = TransferRequest(
            transferId = "tx-7",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("6000.00"),
            currency = "BRL"
        )

        assertThrows<BusinessException.InsufficientBalanceException> {
            validationService.validate(request, validSourceAccount, validDestinationAccount)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando conta de destino estiver inativa`() {
        val inactiveDest = Account(
            accountId = "acc-000",
            balance = BigDecimal("0.00"),
            currency = "BRL",
            status = AccountStatus.INACTIVE,
            customerName = "Conta Encerrada"
        )

        val request = TransferRequest(
            transferId = "tx-8",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-000",
            amount = BigDecimal("100.00"),
            currency = "BRL"
        )

        assertThrows<BusinessException.InactiveAccountException> {
            validationService.validate(request, validSourceAccount, inactiveDest)
        }
    }

    @Test
    fun `deve lancar erro de negocio quando valor da transferencia tiver mais de duas casas decimais`() {
        val request = TransferRequest(
            transferId = "tx-scale-invalid",
            sourceAccountId = "acc-123",
            destinationAccountId = "acc-456",
            amount = BigDecimal("150.755"),
            currency = "BRL"
        )

        val ex = assertThrows<BusinessException.InvalidAmountException> {
            validationService.validate(request, validSourceAccount, validDestinationAccount)
        }
        org.junit.jupiter.api.Assertions.assertTrue(ex.message!!.contains("2 decimal places"))
    }
}
