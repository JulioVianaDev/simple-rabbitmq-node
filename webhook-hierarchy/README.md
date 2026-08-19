# Webhook Hierarchy - Setup Rapido

## Pre-requisitos

- Docker e Docker Compose
- Node.js 18+

## 1. Subir a infraestrutura

```bash
# Na raiz do projeto (onde esta o docker-compose.yaml)
cd simple-rabbitmq-node

# Se ja rodou antes e quer dados limpos no Postgres:
docker compose down -v

# Subir RabbitMQ + PostgreSQL
docker compose up -d
```

Aguarde uns 10 segundos para os containers ficarem saudaveis.

Verifique:
- RabbitMQ Management: http://localhost:15672 (admin/admin)
- PostgreSQL: localhost:5432 (admin/admin, database: webhook_messages)

## 2. Instalar dependencias

```bash
cd webhook-hierarchy
npm install
```

## 3. Executar o init-db manualmente (se necessario)

O `docker-compose.yaml` ja monta o `init-db.sql` automaticamente. Mas se o volume do Postgres ja existia antes, o script nao roda de novo. Nesse caso:

```bash
# Conectar no Postgres e rodar o script manualmente
docker exec -i postgres psql -U admin -d webhook_messages < init-db.sql
```

## 4. Rodar os 3 processos (cada um em um terminal separado)

### Terminal 1 - API Server
```bash
node api-server.js
```
Saida esperada:
```
[*] Connected to RabbitMQ
[*] API Server listening on http://localhost:3001
[*] No database dependency - webhooks are safe even if DB is down
```

### Terminal 2 - Router Worker
```bash
node router-worker.js
```
Saida esperada:
```
[*] Connected to PostgreSQL
[*] Loaded 4 instance->tenant mappings
[*] Connected to RabbitMQ
[*] Publish channel ready
```

### Terminal 3 - Processor Worker
```bash
node processor-worker.js
```
Saida esperada:
```
[*] Connected to RabbitMQ
[*] Processors: 1, my index: 0
```

## 5. Simular webhooks

```bash
node simulate-webhooks.js
```

Isso envia 14 webhooks simulados para `http://localhost:3000/webhook` cobrindo:
- 4 instancias (1234, 1231, abcde, goiaba)
- 2 tenants (xpto, tomat)
- 6 clientes conhecidos + 1 cliente novo (auto-criado)
- 2 eventos de sistema (instance.connected, instance.status)

## 6. Testar escala (opcional)

Abra mais terminais e rode workers adicionais:

```bash
# Terminal 4 - Segundo processor (rebalanceia automaticamente)
node processor-worker.js

# Terminal 5 - Segundo router (opcional, so precisa se tiver muitas instancias)
node router-worker.js
```

Observe nos logs que as filas sao redistribuidas entre os workers.

## 7. Testar graceful shutdown

Pressione `Ctrl+C` em qualquer worker durante o processamento. Observe:
1. Consumers sao cancelados (para de receber mensagens novas)
2. Mensagens em processamento terminam antes do exit
3. Processo sai com codigo 0

## 8. Testar resiliencia do banco

1. Com tudo rodando, pare o Postgres:
   ```bash
   docker stop postgres
   ```
2. Envie webhooks:
   ```bash
   node simulate-webhooks.js
   ```
3. Observe: API Server aceita normalmente (nao depende do banco). Router faz NACK+requeue.
4. Suba o Postgres de volta:
   ```bash
   docker start postgres
   ```
5. Router retoma o processamento automaticamente.

## Monitoramento

- **RabbitMQ Management UI**: http://localhost:15672
  - Aba "Queues" mostra todas as filas criadas, mensagens pendentes, consumers ativos
  - Procure por filas com prefixo `webhooks.raw.` (raw), `xpto.webhooks.` e `tomat.webhooks.` (customer)

## Estrutura dos arquivos

```
webhook-hierarchy/
  config.js              - Configuracoes de conexao (RabbitMQ, PostgreSQL)
  api-server.js          - HTTP server (POST /webhook) → publica na fila raw
  router-worker.js       - Consome raw → resolve tenant/customer → publica na fila do customer
  processor-worker.js    - Consome filas de customer → processa (sua logica de IA aqui)
  simulate-webhooks.js   - Script para enviar webhooks de teste
  init-db.sql            - Schema + dados de seed (tenants, instancias, contatos)
  package.json           - Dependencias (amqplib, pg)
```
