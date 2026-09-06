# Bank Transfer Service — Dry-Run PoC

Microsserviço bancário desenvolvido em **Kotlin (1.9+) com Spring Boot 3.3.x**, responsável pelo processamento assíncrono de transferências internas com alta consistência, atomicidade, idempotência e resiliência.

---

## 🏛️ Arquitetura da Solução

```
+---------------------------+
| API Gateway /             |
| Serviço Externo           |
+---------------------------+
       |
       | publica evento
       v
+---------------------------+       +-------------------+
| Kafka Topic               |       | SQS DLQ           |
| "transfer-requested"      |       | "transfer-failed" |
+---------------------------+       +-------------------+
       |                                     ^
       | consome (adapters.inbound.kafka)    | falhas (adapters.outbound.sqs)
       v                                     |
+---------------------------------------------------------------+
| HEXAGONAL / CLEAN ARCHITECTURE                                |
|                                                               |
|  [adapters.inbound] ---> [domain.port.ProcessTransferUseCase] |
|                                  |                            |
|                                  v                            |
|                     [domain.service.TransferProcessorService] |
|                                  |                            |
|                                  v                            |
|                       [domain.port.* (outbound)]              |
|                                  ^                            |
|  [adapters.outbound] ------------+                            |
+---------------------------------------------------------------+
       |
       +---> DynamoDB (adapters.outbound.dynamodb)
       |
       +---> Kafka Topic "transfer-completed" (adapters.outbound.kafka)
```

### 🧱 Estrutura em 2 Pacotes (Domain & Adapters)
1. **`domain` (Autocontido com Regras & Portas):**
   - `model`: Entidades e VOs de negócio (`Account`, `Transaction`, `TransferRequest`).
   - `service`: Orquestrador de transferência (`TransferProcessorService`) e validação (`TransferValidationService`).
   - `port`: Contratos e interfaces de inversão de dependência (`ProcessTransferUseCase`, `AccountRepositoryPort`, `TransactionRepositoryPort`, `TransferCompletedProducerPort`, `TransferDlqProducerPort`, `MetricsPort`).
   - `event`: Eventos de domínio (`TransferCompletedEvent`, `TransferFailedEvent`).
   - `exception`: Exceções de negócio e falhas transientes (`BusinessException`, `TransientException`).
2. **`adapters` (Infraestrutura e Tecnologias):**
   - `inbound`: Kafka Consumer (`TransferConsumer`), DTOs e extension functions de mapeamento.
   - `outbound`: Repositórios DynamoDB, Kafka Producer, SQS DLQ Producer, métricas Micrometer.
   - `config`: Beans de configuração Spring (AWS, DynamoDB, Kafka, SQS, Micrometer, DomainConfig).
   - **Mapeamentos:** Transformações realizadas nos adaptadores via **extension functions** (`toDomain()`, `toItem()`, `toDomainAccount()`, `toDomainTransactionRecord()`).


### Principais Características Técnicas
1. **Atomicidade e Idempotência:**
   - Execução através de `TransactWriteItems` do DynamoDB: débito na conta de origem, crédito na conta de destino e inserção na tabela `transactions` com condição `attribute_not_exists(transferId)`. Tudo ocorre em uma única operação ACID (all-or-nothing).
2. **Separação de Falhas e Resiliência (Desafio Extra 1 e 2):**
   - **Falhas de Negócio** (saldo insuficiente, conta inexistente ou inativa, moeda inválida): enviadas imediatamente para a DLQ SQS `transfer-failed` e salvas como `FAILED` na tabela `transactions`, sem retries desnecessários.
   - **Erros Transientes** (timeouts, throttling): submetidos a retry automático com **backoff exponencial** (até 3 tentativas) antes de serem encaminhados para a DLQ.
3. **Garantia de Ordenação por Cliente (FIFO) e Particionamento por `sourceAccountId`:**
   - **Ordenação Estrita por Conta:** O Apache Kafka garante a ordem de entrega das mensagens estritamente **no nível de partição**. Ao utilizar o `sourceAccountId` como a chave de partição (`message key`), todas as transações de uma mesma conta são roteadas deterministicamente para a **mesma partição** (via hash Murmur2 padrão do Kafka).
   - **Eliminação de Condições de Corrida (*Race Conditions*):** Cada partição é consumida sequencialmente por apenas uma thread do grupo de consumidores (`concurrency: 3`). Isso impede que duas transferências da mesma conta de origem sejam processadas em paralelo por threads diferentes, eliminando *lost updates* e garantindo que o saldo seja avaliado e debitado na ordem exata de chegada.
   - **Paralelismo Seguro e Balanceamento:** O cluster está configurado com 3 partições. Mensagens de contas distintas (`acc-123`, `acc-456`, `acc-789`) são distribuídas entre partições distintas, permitindo alto throughput com concorrência segura entre clientes.
4. **Logs Estruturados e Observabilidade:**
   - Logs em formato JSON (Logstash Logback Encoder) com `transferId`, `sourceAccountId` e `partitionKey` via MDC (Mapped Diagnostic Context).
   - Métricas customizadas via Micrometer exportadas no endpoint `/actuator/prometheus` (tempo de processamento por transferência e contadores de sucesso/falha).
5. **Infraestrutura Local 100% Automatizada:**
   - Docker Compose provisionando Apache Kafka (KRaft mode, sem zookeeper) e LocalStack (DynamoDB + SQS), já inicializando tabelas e contas de teste.

---

## 📋 Pré-requisitos

- **Java JDK 21** (ou 17+) instalado
- **Docker** e **Docker Compose**

---

## 🚀 Como Rodar o Projeto

### Passo 1: Subir a Infraestrutura Local
No terminal, na raiz do projeto (`bank-transfer-service`):

```bash
docker compose up -d
```

Este comando inicializa:
- **Kafka (Apache Kafka em Modo KRaft)** na porta `localhost:9092`
- **Schema Registry (Confluent)** na porta `localhost:8081`
- **schema-registry-init**: registra automaticamente o schema Avro (`transfer_request_event.avsc`) no subject `transfer-requested-value`
- **kafka-init**: provisiona automaticamente os tópicos `transfer-requested` e `transfer-completed` (3 partições)
- **LocalStack** na porta `localhost:4566`
- **aws-init**: cria automaticamente as tabelas `accounts` e `transactions`, a fila SQS `transfer-failed` e popula as contas pré-definidas:
  - `acc-123`: Saldo R$ 5.000,00 (ACTIVE) — João Silva
  - `acc-456`: Saldo R$ 1.200,50 (ACTIVE) — Maria Santos
  - `acc-789`: Saldo R$ 300,00 (ACTIVE) — Pedro Costa
  - `acc-000`: Saldo R$ 0,00 (INACTIVE) — Conta Encerrada

Aguardar a conclusão dos serviços ***-init*** que criam a infraestrura necessária.

### Passo 2: Executar os Testes Automatizados
Para rodar a suíte de testes unitários e de regras de negócio:

```bash
.\gradlew.bat test
```

### Passo 3: Iniciar a Aplicação
```bash
.\gradlew.bat bootRun
```
A aplicação iniciará na porta `8080`.

---

## 🧪 Testando os Cenários (Execução Direta / Play ▶️)

> **Importante (Formato Avro & Chave de Partição `sourceAccountId`):**
> 1. **Chave de Partição (`sourceAccountId`):** As mensagens no tópico `transfer-requested` utilizam o `sourceAccountId` como chave no formato `chave:valor` (ex: `acc-123:{...}`) para assegurar que todas as operações da mesma conta convirjam para a mesma partição no Kafka e sejam consumidas na ordem estrita de chegada (FIFO).
> 2. **Configuração do `kafka-avro-console-producer`:** Os parâmetros abaixo garantem o roteamento correto:
>    - `--property parse.key=true`: Habilita leitura da chave na entrada padrão.
>    - `--property key.separator=:`: Define `:` como o separador entre a chave de partição e o payload Avro.
>    - `--property key.serializer=org.apache.kafka.common.serialization.StringSerializer`: Serializa a chave como String (compatível com o `StringDeserializer` da aplicação).
> 3. **Tipagem de Valores:** O campo `amount` deve ser informado como string entre aspas (ex: `"150.75"`). Todos os blocos abaixo estão prontos para execução com o botão **Play (▶️)** da IDE no Windows.

---

### 1. Cenário de Sucesso (Transferência Válida com Chave de Partição)
Envie uma transferência de R$ 150,75 da conta `acc-123` para a conta `acc-456` (chave de partição `acc-123`):

```bash
echo 'acc-123:{"transferId":"550e8400-e29b-41d4-a716-446655440000","sourceAccountId":"acc-123","destinationAccountId":"acc-456","amount":"150.75","currency":"BRL","requestedAt":"2025-01-15T10:30:00Z"}' | docker exec -i bank-schema-registry kafka-avro-console-producer --broker-list kafka:29092 --topic transfer-requested --property schema.registry.url=http://localhost:8081 --property value.schema.id=1 --property parse.key=true --property key.separator=: --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```

**Resultado Esperado:**
- Mensagem roteada deterministicamente para a partição de `acc-123`.
- `acc-123` tem o saldo debitado de 5000.00 para 4849.25.
- `acc-456` tem o saldo creditado de 1200.50 para 1351.25.
- Evento publicado no tópico `transfer-completed` com a mesma chave `acc-123`.
- Transação persistida na tabela `transactions` com status `COMPLETED`.

Verificar evento de sucesso publicado no Kafka (exibindo chave e partição):
```bash
docker exec -i bank-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transfer-completed --from-beginning --max-messages 1 --property print.key=true --property print.partition=true
```

---

### 2. Cenário de Idempotência
Clique no **Play (▶️)** do bloco abaixo para reenviar o mesmo payload e verificar que o sistema rejeita duplicatas sem debitar novamente:

```bash
echo 'acc-123:{"transferId":"550e8400-e29b-41d4-a716-446655440000","sourceAccountId":"acc-123","destinationAccountId":"acc-456","amount":"150.75","currency":"BRL","requestedAt":"2025-01-15T10:30:00Z"}' | docker exec -i bank-schema-registry kafka-avro-console-producer --broker-list kafka:29092 --topic transfer-requested --property schema.registry.url=http://localhost:8081 --property value.schema.id=1 --property parse.key=true --property key.separator=: --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```

**Resultado Esperado:**
- A aplicação identifica que o `transferId` já foi processado anteriormente com status `COMPLETED`.
- Nenhum débito ou crédito adicional é realizado.
- Log de aviso informa que a transação duplicada foi ignorada com segurança.

---

### 3. Cenário de Falha de Negócio: Saldo Insuficiente
Tentativa de transferir R$ 99.000,00 da conta `acc-123` (que possui saldo de R$ 4.849,25):

```bash
echo 'acc-123:{"transferId":"a1b2c3d4-e5f6-7890-abcd-111122223333","sourceAccountId":"acc-123","destinationAccountId":"acc-456","amount":"99000.00","currency":"BRL","requestedAt":"2026-09-04T10:35:00Z"}' | docker exec -i bank-schema-registry kafka-avro-console-producer --broker-list kafka:29092 --topic transfer-requested --property schema.registry.url=http://localhost:8081 --property value.schema.id=1 --property parse.key=true --property key.separator=: --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```

**Resultado Esperado:**
- Não há movimentação nos saldos das contas.
- Mensagem enviada para a fila SQS DLQ `transfer-failed`.
- Registro salvo na tabela `transactions` com status `FAILED` e motivo da rejeição.

Verificar mensagem na fila SQS DLQ:
```bash
docker exec bank-localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/transfer-failed
```

---

### 4. Cenário de Falha de Negócio: Conta Inativa
Tentativa de transferir para a conta `acc-000` (encerrada):

```bash
echo 'acc-123:{"transferId":"c3d4e5f6-a7b8-9012-cdef-444455556666","sourceAccountId":"acc-123","destinationAccountId":"acc-000","amount":"100.00","currency":"BRL","requestedAt":"2026-09-04T10:40:00Z"}' | docker exec -i bank-schema-registry kafka-avro-console-producer --broker-list kafka:29092 --topic transfer-requested --property schema.registry.url=http://localhost:8081 --property value.schema.id=1 --property parse.key=true --property key.separator=: --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
```

**Resultado Esperado:**
- Transferência rejeitada imediatamente (`Destination account is not active: acc-000`).
- Encaminhado para a SQS DLQ com status `FAILED`.

---

### 5. Verificação da Distribuição de Partições no Kafka
Para inspecionar como as mensagens com chaves diferentes se distribuem deterministicamente entre as 3 partições (`acc-123` -> Partição 2; `acc-456` -> Partição 1):

```bash
docker exec -i bank-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transfer-requested --from-beginning --property print.key=true --property print.partition=true
```

---

## 📊 Métricas e Monitoramento

Acesse as métricas expostas pelo Micrometer / Prometheus no navegador ou terminal:
- Endpoint: `http://localhost:8080/actuator/prometheus`
- Métricas chave:
  - `bank_transfers_processed_total{status="completed"}`
  - `bank_transfers_processed_total{status="failed"}`
  - `bank_transfers_duration_seconds_max`

Consultar métricas no terminal:
```bash
curl.exe -s http://localhost:8080/actuator/prometheus
```

---

## 🔍 Consultando o DynamoDB Diretamente

Consultar o saldo de uma conta no LocalStack:
```bash
docker exec bank-localstack awslocal dynamodb get-item --table-name accounts --key '{\"accountId\": {\"S\": \"acc-123\"}}'
```

Consultar uma transação:
```bash
docker exec bank-localstack awslocal dynamodb get-item --table-name transactions --key '{\"transferId\": {\"S\": \"550e8400-e29b-41d4-a716-446655440000\"}}'
```

---

## 📑 Documentos Adicionais

- [CODE_REVIEW_LEGACY.md](CODE_REVIEW_LEGACY.md): Code review aprofundado do código legado em produção (`legacy-consumer.kt`) apontando vulnerabilidades críticas e como mitigá-las.
- [TIME_TRACKING.md](TIME_TRACKING.md): Relatório de horas e esforço investido no desenvolvimento da PoC.
