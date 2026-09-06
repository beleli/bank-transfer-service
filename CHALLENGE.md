# Dry-Run Program for Partners — Processamento de Transferências Bancárias

## Introdução

Foi selecionado uma parte da arquitetura do cliente com o objetivo do candidato ter contato com a stack e desafios antes de se conectar no engajamento. Será provido um desenho de arquitetura, uma descrição do resultado esperado e um conjunto de requisitos funcionais e não funcionais a serem considerados. Ao final, é esperado uma demo feita pelo candidato demonstrando o resultado obtido e todo o código fonte gerado deve fazer parte do entregável.

## Descrição

O candidato deve codificar o máximo que conseguir no tempo provido a seguinte arquitetura:

Um microserviço de processamento de transferências internas (entre contas do mesmo banco) que consome eventos de um tópico Kafka, executa a lógica de transferência com persistência em DynamoDB, e publica o resultado. O sistema é core bancário — falhas silenciosas e inconsistências financeiras são inaceitáveis.

## Arquitetura

    ┌──────────────────┐
    │  API Gateway /   │
    │  Serviço Externo │
    └────────┬─────────┘
             │ publica evento
             ▼
    ┌──────────────────────┐         ┌──────────────────────┐
    │   Kafka Topic        │         │   SQS DLQ            │
    │ "transfer-requested" │         │ "transfer-failed"    │
    └────────┬─────────────┘         └──────────────────────┘
             │ consome                         ▲
             ▼                                 │ falhas de negócio
    ┌──────────────────────────────────────────┐
    │  MICROSERVIÇO (Kotlin + Spring Boot)     │
    │  - Valida transferência                  │
    │  - Debita conta origem                   │
    │  - Credita conta destino                 │
    │  - Publica evento de conclusão           │
    └────────┬─────────────────────────────────┘
             │
             ├──────> DynamoDB (tabela: accounts)
             │        DynamoDB (tabela: transactions)
             │
             └──────> Kafka Topic "transfer-completed"

## Evento de Entrada (Kafka topic transfer-requested)

    {
      "transferId": "550e8400-e29b-41d4-a716-446655440000",
      "sourceAccountId": "acc-123",
      "destinationAccountId": "acc-456",
      "amount": 150.75,
      "currency": "BRL",
      "requestedAt": "2025-01-15T10:30:00Z"
    }

## Requisitos Funcionais

1. Consumir eventos do tópico Kafka transfer-requested
2. Validar a transferência: conta origem deve existir e ter saldo suficiente; conta destino deve existir e estar ativa; valor deve ser positivo; moeda deve ser BRL
3. Executar a transferência: debitar conta origem e creditar conta destino de forma **atômica** (não pode debitar sem creditar)
4. Garantir **idempotência**: o mesmo transferId não pode ser processado duas vezes (cenário real: rebalanceamento de partições Kafka)
5. Publicar evento de sucesso no tópico transfer-completed
6. Enviar para SQS DLQ transfer-failed as transferências rejeitadas por regra de negócio, incluindo o motivo da rejeição
7. Persistir o registro da transação com status final (completed/failed) na tabela transactions

## Requisitos Não Funcionais

1. Código da aplicação deve ser desenvolvido em **Kotlin** (1.9+) com **Spring Boot 3.x**
2. Persistência em **DynamoDB** (usar LocalStack ou DynamoDB Local para desenvolvimento)
3. Mensageria com **Kafka** (usar Testcontainers ou docker-compose)
4. Build com **Gradle** (Kotlin DSL preferencial), JDK 17 ou 21
5. Testes automatizados devem cobrir: cenários de sucesso, falhas de negócio, idempotência e atomicidade
6. Logs estruturados (JSON) com transferId como correlation ID
7. Toda infraestrutura local deve subir via docker-compose
8. Sem hardcode de credenciais ou endpoints (configuração externalizada)

## Desafio Extra

Caso consiga implementar todo o escopo anterior:

1. Implementar retry com backoff exponencial para erros transientes (timeout DynamoDB, throttling) — máximo 3 tentativas antes de enviar para DLQ
2. Diferenciar no tratamento de erro: erros de negócio (DLQ imediata, sem retry) vs. erros transientes (retry antes de DLQ)
3. Adicionar métricas de processamento: tempo por transferência e taxa de sucesso/falha
4. Escrever um code review do trecho abaixo (legacy-consumer.kt) — listar os problemas encontrados e propor como corrigiria cada um:

**legacy-consumer.kt — código legado em produção:**

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

## Dados de Exemplo (Accounts)

Para facilitar o setup inicial, considere as seguintes contas pré-populadas:

| accountId | balance | currency | status | customerName |
|-----------|---------|----------|--------|--------------|
| acc-123 | 5000.00 | BRL | ACTIVE | João Silva |
| acc-456 | 1200.50 | BRL | ACTIVE | Maria Santos |
| acc-789 | 300.00 | BRL | ACTIVE | Pedro Costa |
| acc-000 | 0.00 | BRL | INACTIVE | Conta Encerrada |

## Sobre Uso de Ferramentas de IA

Você pode usar ferramentas de IA (Claude, Copilot, Devin, Kiro, etc.) durante o desenvolvimento. Na reunião de demo, faremos perguntas sobre decisões técnicas, trade-offs e cenários hipotéticos. Você deve ser capaz de explicar e defender cada escolha feita no código.

## Entrega Final e Demo

Ao final, o candidato fará uma apresentação do progresso feito e deve entregar um único arquivo Zip com todo o código desenvolvido.

**Entregáveis:**

- Código fonte completo (aplicação + testes)
- docker-compose.yml funcional
- README.md com instruções de como rodar e testar
- Demonstração da solução funcionando (na reunião de demo)
- Arquivo com tempo investido (em horas)

Para esse cenário, o prazo será de uma semana. O candidato deve informar quanto tempo (em horas) foi investido na codificação da solução e no dia da demo informar esse valor.

