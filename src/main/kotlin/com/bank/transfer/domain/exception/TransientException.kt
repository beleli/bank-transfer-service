package com.bank.transfer.domain.exception

class TransientException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
