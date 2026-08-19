# Arquitetura - Webhook Queue Hierarchy

## Visao geral

Sistema de filas hierarquicas no RabbitMQ para processar webhooks de um software de atendimento com IA, multi-tenant, com garantia de ordenacao por cliente e processamento paralelo entre clientes.

```
                         POST /webhook
                              |
                        [api-server]
                     (sem banco de dados)
                              |
                              v
                   webhooks.raw.{instanceId}          CAMADA 1: Raw
                              |
                       [router-worker]
                    (resolve tenant + customer)
                              |
              +---------------+----------------+
              |               |                |
              v               v                v
  xpto.webhooks.1234.bat1   tomat...mango   ...system   CAMADA 2: Customer
              |               |                |
         [processor-worker]  [processor-worker]  ...
          (logica de IA)      (logica de IA)
```

---

## Os 3 processos

### 1. API Server (`api-server.js`)

**O que faz**: Recebe HTTP POST no `/webhook`, extrai o `instanceId` do body JSON e publica na fila `webhooks.raw.{instanceId}`.

**Dependencias**: Apenas RabbitMQ. Zero dependencia de banco de dados.

**Por que nao depende do banco**: Se o PostgreSQL estiver fora, o webhook ainda e enfileirado com seguranca. A resolucao do tenant e customer acontece no proximo estagio (router). Isso garante que **nenhum webhook e perdido** por falha de banco.

**Filas que cria**: `webhooks.raw.{instanceId}` (durable: true, persistent messages).

---

### 2. Router Worker (`router-worker.js`)

**O que faz**: Consome das filas `webhooks.raw.*`, consulta o banco para resolver:
1. Qual tenant e dono daquela instancia (`instances` table)
2. Qual customer enviou aquela mensagem (`contacts` table, por telefone)

Depois publica na fila do customer: `{tenantId}.webhooks.{instanceId}.{customerId}`.

**Se o banco esta fora**: Faz `ch.nack(msg, false, true)` — a mensagem volta para a fila raw e sera retentada. Nada e perdido.

**Eventos sem customer** (instance.connected, instance.status, etc): Sao publicados em `{tenantId}.webhooks.{instanceId}.__system__`.

**Clientes novos** (telefone nao encontrado no banco): Auto-cria um contato com ID `auto_{phone}` e roteia normalmente.

---

### 3. Processor Worker (`processor-worker.js`)

**O que faz**: Consome das filas de customer (`{tenantId}.webhooks.{instanceId}.{customerId}`) e executa a logica de negocio (IA, atualizacao de conversa, resposta ao cliente, etc).

**Onde colocar sua logica**: Na funcao `processWebhook()`. Atualmente tem um `setTimeout` simulando processamento. Substitua pela sua chamada de IA.

---

## Hierarquia de filas

### Nomenclatura

| Tipo | Padrao | Exemplo | Quem produz | Quem consome |
|------|--------|---------|-------------|--------------|
| Raw | `webhooks.raw.{instanceId}` | `webhooks.raw.1234` | api-server | router-worker |
| Customer | `{tenantId}.webhooks.{instanceId}.{customerId}` | `xpto.webhooks.1234.bat1` | router-worker | processor-worker |
| System | `{tenantId}.webhooks.{instanceId}.__system__` | `xpto.webhooks.1234.__system__` | router-worker | processor-worker |

### Como as filas sao identificadas por tipo

O codigo diferencia as filas contando as partes separadas por `.`:

- **3 partes**, comecando com `webhooks.raw`: fila raw (router consome)
- **4 partes**, segunda parte `webhooks`: fila de customer ou system (processor consome)

### Criacao dinamica

As filas sao criadas sob demanda com `ch.assertQueue()`:
- O api-server cria filas raw quando recebe um webhook de uma instancia nova
- O router cria filas de customer quando identifica um customer novo
- `assertQueue` e idempotente — se a fila ja existe, nao faz nada

### Durabilidade

Todas as filas de dados sao `durable: true` com mensagens `persistent: true`:
- Se o RabbitMQ reiniciar, as filas e mensagens sobrevivem no disco
- Filas de coordenacao (heartbeat) sao `durable: false` com TTL — sao descartaveis

---

## Conceitos do RabbitMQ usados

### Channel

Um channel e um canal logico dentro de uma conexao TCP com o RabbitMQ. Cada channel e independente — tem seu proprio `prefetch`, seus proprios consumers, seus proprios ACKs.

**Nesta arquitetura**: Cada fila de customer tem seu PROPRIO channel. Isso e fundamental porque:
- `prefetch(1)` e por channel, nao por conexao
- Se dois customers compartilhassem o mesmo channel com `prefetch(1)`, so um dos dois receberia mensagem por vez — anulando o paralelismo

### prefetch(1)

`ch.prefetch(1)` diz ao RabbitMQ: "nao me entregue a proxima mensagem deste channel ate eu dar ACK na atual".

**Efeito pratico**: Garante que mensagens do mesmo customer sao processadas uma por vez, em ordem. Se uma mensagem leva 5 segundos para processar (chamada de IA), a proxima mensagem daquele customer so e entregue depois dos 5 segundos + ACK.

**Por que nao prefetch(0) ou prefetch(10)**: `prefetch(0)` = sem limite, RabbitMQ entrega tudo de uma vez, perdendo a garantia de ordem. `prefetch(10)` = entrega 10 de uma vez, mesmo problema. Para ordenacao por customer, tem que ser 1.

### ACK (acknowledge)

`ch.ack(msg)` confirma para o RabbitMQ que a mensagem foi processada com sucesso. So depois do ACK a mensagem e removida da fila.

**Nesta arquitetura**: O ACK so e dado DEPOIS de:
- No router: a mensagem ter sido publicada na fila do customer
- No processor: a logica de processamento (IA) ter terminado

Se o processo morrer antes do ACK, o RabbitMQ reentrega a mensagem para outro consumer.

### NACK + requeue

`ch.nack(msg, false, true)` rejeita a mensagem e pede para o RabbitMQ devolve-la para a fila.

**Quando e usado**:
- Router nao consegue consultar o banco (DB fora do ar) → NACK + requeue na fila raw
- Worker recebe mensagem durante shutdown → NACK + requeue para outro worker pegar

O terceiro parametro `true` e o `requeue`. Se fosse `false`, a mensagem seria descartada (ou iria para dead-letter queue, se configurada).

### Consumer Tag

Cada consumer registrado no RabbitMQ recebe um `consumerTag` unico (ex: `amq.ctag-xyz123`). Esse tag e usado para:
- `ch.cancel(consumerTag)`: para de receber mensagens, mas o channel continua aberto para ACK/NACK mensagens ja entregues
- Identificacao no RabbitMQ Management UI

**No graceful shutdown**: Usamos `ch.cancel()` em vez de `ch.close()` primeiro, justamente para poder dar ACK nas mensagens em voo antes de fechar.

### assertQueue

`ch.assertQueue(name, options)` declara uma fila. Se ja existe, verifica que as opcoes sao compativeis. Se nao existe, cria.

**Opcoes usadas**:
- `durable: true`: Fila sobrevive ao restart do RabbitMQ (salva em disco)
- `persistent: true` (nas mensagens): Mensagem e salva em disco junto com a fila
- `x-message-ttl: 30000` (so na fila de coordenacao): Mensagens expiram em 30 segundos

### Management API

API HTTP do RabbitMQ (porta 15672) que permite consultar filas, consumers, conexoes, etc.

**Nesta arquitetura**: Usada para:
1. **Descobrir filas dinamicamente**: `GET /api/queues` retorna todas as filas existentes. Os workers filtram pelo padrao de nome para saber quais filas consumir.
2. **Coordenacao entre workers**: Nao seria necessaria se usassemos um service registry externo, mas funciona bem para este caso.

---

## Coordenacao entre workers (consistent hashing)

### O problema

Se temos 3 processor workers e 100 filas de customer, como dividir sem que 2 workers consumam a mesma fila?

### A solucao

Cada worker faz:

```
hash(nomeDaFila) % totalDeWorkers === meuIndice ?
  → sim: consumir
  → nao: ignorar
```

O hash e determinístico (MD5 do nome da fila), entao todos os workers concordam sobre quem consome qual fila sem precisar de comunicacao entre eles.

### Descoberta de workers

Workers publicam heartbeats em uma fila de coordenacao (`router-coordination` ou `processor-coordination`):
1. A cada 5 segundos, cada worker publica `{ workerId, timestamp }` com TTL de 30 segundos
2. A cada 10 segundos, cada worker le todas as mensagens da fila de coordenacao para descobrir quais workers estao ativos
3. Com a lista de workers ativos (sorted), cada um calcula seu indice e quais filas deve consumir

### Rebalanceamento

Quando um worker novo sobe ou um worker morre:
- O total de workers muda
- `hash(fila) % novoTotal` muda para algumas filas
- Workers que nao devem mais consumir uma fila fazem `stopConsumer()` (cancel + close channel)
- Workers que devem consumir uma nova fila fazem `createConsumer()`
- O rebalanceamento acontece automaticamente no proximo ciclo de discovery (10 segundos)

### Limitacao

Consistent hashing simples (`% N`) causa redistribuicao de muitas filas quando N muda. Com 100 filas e mudanca de 3→4 workers, cerca de 75% das filas mudam de dono. Para producao com muitos workers, considere consistent hashing com virtual nodes (hash ring).

---

## Graceful Shutdown

### O problema

Se um worker morre no meio do processamento de uma mensagem, o que acontece?

- Se a mensagem ja foi processada (IA respondeu ao cliente) mas o ACK nao foi dado → RabbitMQ reentrega → processamento duplicado
- Se a mensagem nao foi processada → RabbitMQ reentrega → tudo ok

### A solucao: shutdown em 3 etapas

```
SIGINT / SIGTERM recebido
         |
  [1] ch.cancel(consumerTag) em todas as filas
      → RabbitMQ PARA de enviar mensagens novas
      → Channels continuam abertos para ACK/NACK
         |
  [2] Esperar inFlightCount chegar a 0
      → Mensagens em processamento terminam normalmente
      → Cada uma da ACK quando termina
      → Checagem a cada 500ms
      → Timeout de 30s: se travou, forca o exit
         |
  [3] Fechar channels, conexao, banco
      → process.exit(0)
```

### Mensagens que chegam durante o shutdown

A flag `shuttingDown = true` e checada no callback do consumer. Se uma mensagem e entregue entre o SIGINT e o `ch.cancel()` (janela de milissegundos), ela recebe `ch.nack(msg, false, true)` — volta para a fila e outro worker pega.

### Timeout de seguranca

Se alguma mensagem travar (query infinita, IA sem resposta), o shutdown forca apos 30 segundos. As mensagens sem ACK sao automaticamente reenfileiradas pelo RabbitMQ quando a conexao fecha.

---

## Resiliencia

### RabbitMQ cai

Tudo para. Quando volta, as filas `durable` com mensagens `persistent` ainda estao la. Workers reconectam.

### PostgreSQL cai

- **API Server**: Continua funcionando normalmente. Webhooks sao enfileirados na fila raw.
- **Router Worker**: Faz NACK + requeue em cada mensagem que tenta processar. Mensagens acumulam na fila raw. Quando o banco volta, retoma o processamento.
- **Processor Worker**: Nao depende de banco (a menos que sua logica de IA precise). Continua processando filas de customer que ja existem.

### Worker morre (kill -9, OOM, crash)

- Mensagens sem ACK sao reenfileiradas pelo RabbitMQ automaticamente
- Outros workers assumem as filas do worker morto no proximo ciclo de discovery (10 segundos)
- Risco: processamento duplicado se a logica de IA ja tinha executado antes do crash. Solucao: idempotencia na logica de negocio.

---

## Fluxo completo de uma mensagem

```
1. Cliente "bat1" envia "Ola" pelo WhatsApp para instancia 1234

2. Provedor de WhatsApp envia webhook para sua API:
   POST /webhook
   { "instanceId": "1234", "event": "message.received",
     "remoteJid": "5511999990001@s.whatsapp.net", "body": "Ola" }

3. api-server.js:
   - Extrai instanceId = "1234"
   - Publica em: webhooks.raw.1234
   - Responde 200 OK

4. router-worker.js:
   - Consome de: webhooks.raw.1234
   - Query: SELECT tenant_id FROM instances WHERE instance_id = '1234'
     → tenant = "xpto"
   - Query: SELECT id FROM contacts WHERE phone = '5511999990001' AND instance_id = '1234'
     → customer = "bat1"
   - Publica em: xpto.webhooks.1234.bat1
   - ACK na fila raw

5. processor-worker.js:
   - Consome de: xpto.webhooks.1234.bat1
   - Executa processWebhook() → sua logica de IA
   - ACK na fila do customer

6. Enquanto bat1 esta sendo processado:
   - Outra mensagem de bat1 ESPERA na fila (prefetch 1)
   - Mensagem de cli2 e processada EM PARALELO (channel diferente)
   - Mensagem de mango (outro tenant) e processada EM PARALELO
```

---

## Escalando

| Componente | Como escalar | Quando escalar |
|---|---|---|
| API Server | Load balancer na frente de N instancias | Muitas requisicoes HTTP/s |
| Router Worker | Rodar N processos (auto-balanceia) | Muitas instancias com alto volume de webhooks |
| Processor Worker | Rodar N processos (auto-balanceia) | Muitos customers simultaneos ou processamento lento (IA) |

O gargalo tipico e o processor (logica de IA e lenta). Rodar mais processors distribui as filas de customer entre mais cores/maquinas.

---

## FAQ

**Posso ter 10.000 filas de customer?**
Sim. RabbitMQ lida bem com milhares de filas. Cada fila vazia consome pouca memoria. O custo real e o numero de channels abertos nos processors (1 por fila consumida), que e limitado pela conexao TCP (~65k channels por conexao teoricamente, mas na pratica mantenha abaixo de alguns milhares por processo).

**E se um customer ficar inativo por dias?**
A fila continua existindo mas vazia. Voce pode implementar uma rotina de limpeza que deleta filas vazias e ociosas apos X dias via Management API (`DELETE /api/queues/{vhost}/{name}`).

**Preciso de Exchange?**
Nao nesta arquitetura. Usamos `sendToQueue()` diretamente (default exchange). Se no futuro precisar de fan-out (enviar a mesma mensagem para multiplos consumers), ai sim use exchanges com bindings.

**Como garantir idempotencia?**
Adicione um `messageId` unico no webhook (UUID) e verifique no processor se ja foi processado antes de executar a logica. Isso protege contra redelivery apos crash.
