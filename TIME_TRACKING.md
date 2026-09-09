# Registro de Tempo Investido — PoC Core Banking Transfer Service

**Candidato / Desenvolvedor:** Carlos Alberto Beleli Junior
**Projeto:** Microsserviço de Processamento de Transferências Bancárias  
**Tempo Total de Investimento:** 26.0 horas

---

## ⏱️ Detalhamento das Horas por Etapa

| Etapa | Atividades Realizadas | Tempo |
| :--- | :--- | :---: |
| **1. Análise e Arquitetura** | Levantamento de requisitos, desenho da arquitetura hexagonal (Portas e Adaptadores com Domínio puro), modelagem do fluxo Kafka/SQS e garantia de idempotência. | 2.0 h |
| **2. Setup de Infraestrutura Local** | Configuração do `docker-compose.yml` com Apache Kafka KRaft (sem Zookeeper), Confluent Schema Registry (`schema-registry-init`), LocalStack (DynamoDB + SQS) e volumes centralizados em `./docker/`. | 2.0 h |
| **3. Modelagem de Domínio e Contratos Avro** | Implementação das entidades de domínio puro (`Account`, `Transaction`, `TransferRequest`), portas de negócio, schema Avro canônico (`transfer_request_event.avsc`) e rotina Gradle de compilação de código tipado. | 2.0 h |
| **4. Persistência e Transacionalidade (DynamoDB)** | Desenvolvimento dos adaptadores de persistência com `TransactWriteItems` (débito, crédito e registro atômicos em operação ACID única) e extension functions de mapeamento. | 3.5 h |
| **5. Mensageria, Schema Registry e Resiliência** | Consumidor Kafka com `TransferRequestEvent`, configuração de `ErrorHandlingDeserializer` contra poison pills, produtores Kafka/SQS e segregação de erros transientes (Retry) vs. negócio (DLQ imediata). | 3.5 h |
| **6. Observabilidade e Logs** | Configuração de logs estruturados em JSON via Logstash Logback Encoder com MDC (`transferId`), métricas de latência e contadores Micrometer expostos para Prometheus. | 2.0 h |
| **7. Testes Automatizados** | Implementação de 19 testes unitários com Mockk e JUnit 5 cobrindo regras de negócio, saldo insuficiente, concorrência, idempotência, desserialização Avro e DLQ. | 2.5 h |
| **8. Code Review Legado e Documentação** | Análise crítica de segurança e resiliência do código `legacy-consumer.kt`, elaboração do code review e guia prático com execução direta no `README.md`. | 0.5 h |
| **9. Melhoria da Arquitetura e Resiliência** | Refatoração da camada de domínio (`TransferProcessorService`), tratamento de conflitos de concorrência transacional no DynamoDB (`TransactionConflictException`), implementação de reconciliador periódico (Padrão Outbox) com lock distribuído via `ShedLock` e ampliação da suíte de testes unitários de resiliência. | 8.0 h |
| **Total Geral** | | **26.0 h** |

---

## 🎯 Decisões Técnicas e Trade-offs para a Apresentação / Demo

1. **Arquitetura Hexagonal (Ports & Adapters) com Domínio Puro:**
   - O pacote `domain` é 100% autocontido, sem dependências ou anotações de frameworks (Spring, Jackson, AWS SDK, Avro).
   - Comunicação através de portas de entrada (`ProcessTransferUseCase`) e de saída (`*Port`), com conversões realizadas na borda via Kotlin Extension Functions.

2. **Garantia de ACID e Idempotência no DynamoDB:**
   - Utilização de `TransactWriteItems` para executar débito, crédito e inserção de transação com condição de unicidade (`attribute_not_exists(transferId)`) de forma atômica e nativa.
   - Dispensa a necessidade de locks distribuídos complexos (como Redis/Redlock) para transferências internas.

3. **Governança de Contratos com Apache Avro & Schema Registry:**
   - Validação e evolução de schemas no tópico Kafka garantida pelo Confluent Schema Registry.
   - Desserialização fortemente tipada em tempo de compilação, combinada com `ErrorHandlingDeserializer` para proteção contra *poison pills*.

4. **Separação de Falhas:**
   - **Falhas de Negócio:** Rejeição imediata sem retries (ex: saldo insuficiente, conta inativa), persistência com status `FAILED` e despacho para a fila SQS DLQ `transfer-failed`.
   - **Falhas Transientes:** Retry com backoff exponencial antes de encaminhar para a DLQ, assegurando tolerância a falhas momentâneas.

5. **Garantia de Ordenação por Cliente (FIFO) via Chave de Partição (`sourceAccountId`):**
   - Ao definir `sourceAccountId` como a chave de partição no Kafka, todas as solicitações da mesma conta convergem deterministicamente para a mesma partição.

6. **Resiliência Transacional e Padrão Outbox com Lock Distribuído (`ShedLock`):**
   - Tratamento especializado de conflitos de concorrência (`TransactionConflict`) no DynamoDB como falha transiente, permitindo retries automáticos seguros.
   - Implementação do `OutboxReconciliationScheduler` com lock distribuído via `ShedLock` para garantir a publicação eventual no Kafka de transações concluídas no banco, operando de forma segura em ambiente multi-instâncias.
