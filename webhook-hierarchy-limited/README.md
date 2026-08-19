# Webhook Hierarchy Limited

Mesma arquitetura do `webhook-hierarchy`, com tres features adicionais:

1. **Notificacao instantanea de filas novas** (zero delay)
2. **Limite de consumers por worker** (MAX_CONSUMERS_PER_WORKER = 500)
3. **Auto-limpeza de filas inativas** (x-expires = 5 min)

## O que muda vs webhook-hierarchy

### 1. Notificacao instantanea (sem polling delay)

No `webhook-hierarchy`, o processor descobria filas novas a cada 10 segundos via Management API. Uma mensagem de um customer novo ficava parada 0-10s antes de ser processada.

**Agora:** O router avisa TODOS os processors no instante em que cria uma fila nova, via **fanout exchange**.

```
Router cria fila xpto.webhooks.1234.novoCliente
  |
  +---> publish no exchange fanout "processor-new-queue-notify"
  |
  +---> RabbitMQ copia a mensagem para TODAS as filas exclusivas dos workers:
  |       Worker 0 (amq.gen-abc) ← recebe
  |       Worker 1 (amq.gen-xyz) ← recebe
  |       Worker 2 (amq.gen-def) ← recebe
  |
  v
Cada worker checa: hashToInt("xpto...novoCliente") % 3 === meuIndex?
  Worker 0: sim → createConsumer() IMEDIATAMENTE
  Worker 1: nao → ignora
  Worker 2: nao → ignora
```

**Por que fanout e nao uma fila normal?**
Uma fila normal distribui round-robin — se o Worker 1 recebe a notificacao
mas a fila pertence ao Worker 0, ele ignora e o Worker 0 NUNCA recebe.
Fanout garante que TODOS recebem, e cada um filtra localmente.

**Filas exclusivas:**
Cada processor cria uma fila com `exclusive: true` (nome gerado pelo RabbitMQ).
Exclusive = so esta conexao pode usar + auto-deletada quando desconecta.
Se o worker morre, a fila some automaticamente. Zero lixo.

**O discovery agora roda a cada 30 segundos** (era 10) e serve apenas como fallback:
- Rebalanceamento quando workers entram/saem
- Cleanup de consumers orfaos
- Pegar filas que a notificacao perdeu (ex: processor reiniciou)

### 2. Limite de consumers

Cada customer queue consome 1 channel do RabbitMQ (~50-100KB RAM). Com 10.000 customers ativos, seriam 10.000 channels = ~1GB RAM so de channels. O limite de 500 consumers por worker evita isso.

**Como funciona:**

```
Notificacao chega para fila nova:
  - Se active < 500 → inicia consumer imediatamente
  - Se active >= 500 → loga "at limit", espera proximo ciclo de discovery

No ciclo de discovery (cada 30s):
  - Ordena filas: COM mensagens primeiro (prioridade)
  - Se no limite: para consumers de filas VAZIAS para abrir slots
  - Inicia consumers para filas COM mensagens nos slots abertos
```

**O que acontece com filas alem do limite:**
- Ficam no RabbitMQ com mensagens acumulando
- No proximo ciclo de discovery, se um slot liberar (fila ficou vazia), uma fila com mensagens entra no lugar
- Mensagens NAO sao perdidas — ficam na fila esperando

### 3. Auto-limpeza de filas (x-expires)

Filas de customer sao criadas dinamicamente. Sem limpeza, se 50.000 customers diferentes enviarem mensagem ao longo de meses, voce teria 50.000 filas no RabbitMQ (a maioria vazia).

**Solucao: `x-expires` no RabbitMQ**

```
Fila criada com: { arguments: { "x-expires": 300000 } }

x-expires = 300000ms = 5 minutos

Regra do RabbitMQ:
- Timer comeca quando a fila NAO tem consumers E NAO tem mensagens
- Se um consumer conectar OU uma mensagem chegar → timer reseta
- Se 5 minutos passam sem consumer e sem mensagem → RabbitMQ DELETA a fila
```

**Importante:** O processor tambem precisa usar o MESMO `x-expires` no `assertQueue`. Se o router cria com `x-expires: 300000` e o processor tenta `assertQueue` sem `x-expires`, o RabbitMQ rejeita (argumentos incompativeis).

## Setup

```bash
# Mesma infra do webhook-hierarchy (docker compose ja esta rodando)
cd webhook-hierarchy-limited
npm install
```

## Rodando

```bash
# Terminal 1
node api-server.js

# Terminal 2
node router-worker.js

# Terminal 3
node processor-worker.js
```

## Testando a notificacao instantanea

1. Rode os 3 processos
2. Envie um webhook de um customer novo:

```bash
curl -X POST http://localhost:3001/webhook \
  -H "Content-Type: application/json" \
  -d '{"instanceId":"1234","event":"message.received","remoteJid":"5599999999999@s.whatsapp.net","body":"teste"}'
```

3. Observe nos logs do processor: `[NOTIFY] New queue xpto.webhooks.1234.auto_5599999999999 → starting consumer immediately`
4. A mensagem e processada em menos de 1 segundo (antes era ate 10s)

## Testando o limite de consumers

```bash
# Teste normal (16 webhooks)
node simulate-webhooks.js

# Stress: 50 customers x 3 mensagens cada
node simulate-webhooks.js stress 50

# Stress: 600 customers (excede o limite de 500)
node simulate-webhooks.js stress 600
```

Com `stress 600`:
- Router cria 600 customer queues e notifica o processor para cada uma
- Processor aceita 500 via notificacao, as outras 100 recebem "at limit"
- No proximo discovery, conforme as 500 esvaziam, as 100 pendentes entram
- Observe nos logs: `Active: 500/500 | Waiting: 100`

## Testando a limpeza de filas

1. Rode `node simulate-webhooks.js` e espere processar tudo
2. Pare o processor (Ctrl+C) — nao ha mais consumers nas filas
3. As filas estao vazias (tudo processado) e sem consumers
4. Espere 5 minutos
5. Veja no RabbitMQ Management UI (http://localhost:15672 → Queues):
   - As customer queues desapareceram automaticamente
   - As raw queues continuam (nao tem x-expires)

Para testar mais rapido, altere o `CUSTOMER_QUEUE_EXPIRES_MS` em `config.js` para `30000` (30 segundos).

## Configuracao

Em `config.js`:

```js
DISCOVERY_INTERVAL: 30000,        // discovery agora eh fallback (30s)
MAX_CONSUMERS_PER_WORKER: 500,    // max customer queues per worker
CUSTOMER_QUEUE_EXPIRES_MS: 300000, // 5 min TTL for idle queues
NOTIFY_EXCHANGE: "processor-new-queue-notify", // fanout exchange
```

## Queries de verificacao

Mesmas queries do `webhook-hierarchy/QUERIES.md`:

```bash
# Ordem esta correta?
docker exec -i postgres psql -U admin -d webhook_messages \
  -c "SELECT * FROM v_order_check;"

# Qual worker processou o que?
docker exec -i postgres psql -U admin -d webhook_messages \
  -c "SELECT * FROM v_worker_distribution;"

# Limpar para novo teste
docker exec -i postgres psql -U admin -d webhook_messages \
  -c "TRUNCATE processed_messages RESTART IDENTITY;"
```
