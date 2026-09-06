# Code Review: `legacy-consumer.kt`

**Arquivo analisado:** `legacy-consumer.kt`  
**Autor original:** Código legado em produção  
**Severidade Geral:** 🔴 **CRÍTICA** (Risco de perdas financeiras, dados inconsistentes e falhas silenciosas)

---

## Código Analisado

```kotlin
@Component
class TransferConsumer(
    private val accountRepository: AccountRepository,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @KafkaListener(topics = ["transfer-requested"], groupId = "transfer-service")
    fun handle(message: String) {
        try {
            val transfer = ObjectMapper().readValue(message, TransferRequest::class.java)
            logger.info("Processing transfer: $transfer")

            val source = accountRepository.findById(transfer.sourceAccountId).get()
            val destination = accountRepository.findById(transfer.destinationAccountId).get()

            source.balance = source.balance - transfer.amount
            accountRepository.save(source)

            destination.balance = destination.balance + transfer.amount
            accountRepository.save(destination)

            kafkaTemplate.send("transfer-completed", transfer.toString())
            logger.info("Transfer ${transfer.transferId} completed for user ${transfer.sourceAccountId}")

        } catch (e: Exception) {
            logger.error("Error processing transfer", e)
        }
    }
}
```

---

## 1. Problemas Encontrados e Impactos

### 🔴 1. Falta de Atomicidade (Risco de Inconsistência Financeira Grave)
- **Problema:** O débito na conta de origem (`accountRepository.save(source)`) e o crédito na conta de destino (`accountRepository.save(destination)`) são feitos como duas operações de banco de dados independentes e separadas.
- **Impacto:** Se o processo falhar, a conexão for interrompida ou a aplicação for reiniciada após o primeiro `save` e antes do segundo, o dinheiro sairá da conta de origem mas **nunca chegará à conta de destino**. Em core bancário, a perda de integridade transacional é inaceitável.
- **Como corrigir:** No DynamoDB, executar as duas mutações dentro de um único comando `TransactWriteItems`, garantindo semântica ACID (*all-or-nothing*). Se qualquer etapa falhar, nenhuma alteração é persistida.

---

### 🔴 2. Ausência de Idempotência (Duplicações por Rebalanceamento do Kafka)
- **Problema:** O Kafka opera por padrão com semântica de entrega *at-least-once*. Durante rebalanceamentos de partições, timeouts de heartbeat do consumidor ou falhas de rede, o mesmo evento de transferência pode ser entregue duas ou mais vezes.
- **Impacto:** Como o código não verifica se o `transferId` já foi processado anteriormente, cada reentrega debitará novamente a conta de origem e creditará a de destino, causando débitos e créditos duplicados.
- **Como corrigir:** Persistir o `transferId` em uma tabela `transactions` usando uma restrição de unicidade (`attribute_not_exists(transferId)` no `TransactWriteItems`). Além disso, verificar no início do processamento se o `transferId` já existe, ignorando o processamento duplicado caso já tenha sido concluído.

---

### 🔴 3. Uso Inseguro de `.get()` em `Optional` / Nullability
- **Problema:** O código utiliza `accountRepository.findById(...).get()` diretamente.
- **Impacto:** Se a conta de origem ou destino não for encontrada no banco de dados, `NoSuchElementException` ou `NullPointerException` será disparado, interrompendo o fluxo e caindo no bloco genérico de catch.
- **Como corrigir:** Tratar ausência explicitamente (ex: `findById(id) ?: throw AccountNotFoundException(id)`), permitindo classificar o erro como regra de negócio violada e direcioná-lo para a fila de DLQ.

---

### 🔴 4. Falta Completa de Validações de Regra de Negócio
- **Problema:** O código não realiza nenhuma validação prévia dos dados antes de executar as mutações:
  - Não verifica se o saldo da conta de origem é suficiente (`source.balance >= transfer.amount`).
  - Não verifica se as contas de origem e destino estão ativas.
  - Não verifica se o valor a ser transferido é positivo (`amount > 0`).
  - Não valida se a moeda recebida é suportada (`currency == "BRL"`).
  - Não valida se a conta de origem e destino são diferentes.
- **Impacto:** Contas podem ficar com saldo negativo não autorizado, transferências podem ser feitas para contas encerradas/bloqueadas e valores inválidos (como negativos ou zero) podem corromper os saldos.
- **Como corrigir:** Criar um serviço de validação (`TransferValidationService`) que verifique todas as condições antes de prosseguir com qualquer operação de banco de dados.

---

### 🔴 5. Falha Silenciosa (Swallowing Exception) e Ausência de DLQ
- **Problema:** O bloco `catch (e: Exception)` apenas faz `logger.error("Error processing transfer", e)` e encerra a execução sem relançar o erro nem enviar a mensagem para uma fila de Dead Letter Queue (DLQ).
- **Impacto:** O Kafka entenderá que o lote de mensagens foi processado com sucesso e fará commit do offset. A mensagem falhada é **perdida silenciosamente**. Ninguém no time de suporte, produto ou o cliente saberá por que a transferência não foi concluída.
- **Como corrigir:**
  - Classificar as exceções em **Erros de Negócio** (ex: saldo insuficiente, conta inativa) e **Erros Transientes** (ex: timeout de rede, throttling do DynamoDB).
  - Erros de negócio devem ser registrados como `FAILED` na tabela de transações e enviados imediatamente para a fila SQS DLQ `transfer-failed` com o motivo da rejeição.
  - Erros transientes devem passar por mecanismo de retry com backoff exponencial (ex: até 3 tentativas) antes de serem encaminhados para a DLQ.

---

### 🟠 6. Concorrência e *Lost Updates* (Condição de Corrida)
- **Problema:** A leitura `source.balance` e a escrita `source.balance - transfer.amount` não possuem nenhum bloqueio otimista ou condicional.
- **Impacto:** Se duas transferências da mesma conta de origem forem processadas simultaneamente em threads ou instâncias diferentes, uma sobregravará o saldo da outra (*lost update*).
- **Como corrigir:** No DynamoDB, utilizar a expressão de atualização condicional `SET balance = balance - :amount` com a condição `balance >= :amount` e controle de versão, garantindo linearizabilidade.

---

### 🟠 7. Ineficiência de Alocação de Recursos (`ObjectMapper`)
- **Problema:** A instrução `ObjectMapper().readValue(...)` cria uma nova instância de `ObjectMapper` a cada mensagem consumida.
- **Impacto:** A instanciação do `ObjectMapper` é computacionalmente pesada e aloca muitos metadados de introspecção na memória. Sob alta taxa de eventos, isso causa sobrecarga de Garbage Collection (GC) e degrada o throughput.
- **Como corrigir:** Injetar o bean singleton de `ObjectMapper` gerenciado pelo Spring Container.

---

### 🟡 8. Serialização Imprópria no Envio do Evento de Sucesso
- **Problema:** `transfer.toString()` é enviado como payload para o tópico `transfer-completed`.
- **Impacto:** `transfer.toString()` gera uma representação em texto própria da classe Kotlin `TransferRequest(...)`, e **não** um JSON válido. Os consumidores downstream que esperam JSON quebrarão ao tentar desserializar a mensagem.
- **Como corrigir:** Criar um evento de domínio dedicado (`TransferCompletedEvent`) e serializá-lo adequadamente em JSON utilizando `objectMapper.writeValueAsString(event)`.


## 2. Código Corrigido e Proposto

Abaixo está a versão corrigida da camada de consumo, delegando as responsabilidades de forma desacoplada para serviços especializados:

```kotlin
@Component
class TransferConsumer(
    private val transferProcessorService: TransferProcessorService,
    private val dlqProducer: TransferDlqProducer,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @KafkaListener(topics = ["transfer-requested"], groupId = "transfer-service")
    fun handle(message: String) {
        var transferId: String? = null
        try {
            val transfer = objectMapper.readValue(message, TransferRequest::class.java)
            transferId = transfer.transferId

            // Injeção de correlation ID no contexto de logs
            MDC.put("transferId", transfer.transferId)
            MDC.put("correlationId", transfer.transferId)
            MDC.put("sourceAccountId", transfer.sourceAccountId)

            logger.info("Iniciando consumo de transferência transferId=${transfer.transferId}")

            // Delega para o serviço de orquestração com retry e persistência atômica
            transferProcessorService.processTransfer(transfer)

        } catch (e: Exception) {
            logger.error("Falha ao consumir mensagem: ${e.message}", e)
            val fallbackId = transferId ?: UUID.randomUUID().toString()
            dlqProducer.sendToDlq(
                TransferFailedEvent(
                    transferId = fallbackId,
                    sourceAccountId = "UNKNOWN",
                    destinationAccountId = "UNKNOWN",
                    amount = BigDecimal.ZERO,
                    currency = "BRL",
                    reason = "Malformed payload or unrecoverable error: ${e.message}"
                )
            )
        } finally {
            MDC.clear()
        }
    }
}
```

E no `TransferProcessorService` / `AccountRepository`:
```kotlin
// Operação atômica no DynamoDB via TransactWriteItems:
fun executeAtomicTransfer(...) {
    val debit = Update.builder()
        .tableName("accounts")
        .key(mapOf("accountId" to AttributeValue.builder().s(sourceAccountId).build()))
        .updateExpression("SET balance = balance - :amount")
        .conditionExpression("attribute_exists(accountId) AND balance >= :amount AND #status = :active")
        .build()

    val credit = Update.builder()
        .tableName("accounts")
        .key(mapOf("accountId" to AttributeValue.builder().s(destinationAccountId).build()))
        .updateExpression("SET balance = balance + :amount")
        .conditionExpression("attribute_exists(accountId) AND #status = :active")
        .build()

    val recordTx = Put.builder()
        .tableName("transactions")
        .item(...)
        .conditionExpression("attribute_not_exists(transferId)") // Idempotência
        .build()

    dynamoDbClient.transactWriteItems(...)
}
```
