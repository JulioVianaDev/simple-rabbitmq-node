# Queries para verificar o processamento

Todas as queries rodam no PostgreSQL. Para executar de fora do container:

```bash
docker exec -i postgres psql -U admin -d webhook_messages
```

Ou para rodar uma query direto do terminal:

```bash
docker exec -i postgres psql -U admin -d webhook_messages -c "QUERY AQUI"
```

---

## 1. Ordem das mensagens esta correta?

```sql
SELECT * FROM v_order_check;
```

Resultado esperado:

```
 tenant_id | instance_id | customer_id | total_messages | all_in_order | out_of_order_count
-----------+-------------+-------------+----------------+--------------+--------------------
 xpto      | 1234        | bat1        |              5 | t            |                  0
 xpto      | 1234        | cli2        |              2 | t            |                  0
 tomat     | abcde       | mango       |              2 | t            |                  0
```

- **all_in_order = t**: mensagens foram processadas na ordem correta (sequence sempre crescente)
- **all_in_order = f**: PROBLEMA - alguma mensagem foi processada fora de ordem
- **out_of_order_count**: quantas mensagens estao fora de ordem

---

## 2. Qual worker processou o que?

```sql
SELECT * FROM v_worker_distribution;
```

Resultado esperado:

```
         worker_id          |            queue_name             | messages_processed |       first_at        |        last_at
----------------------------+-----------------------------------+--------------------+-----------------------+-----------------------
 processor-Dev03-1234       | xpto.webhooks.1234.bat1           |                  5 | 2026-08-19 10:00:01   | 2026-08-19 10:00:05
 processor-Dev03-1234       | tomat.webhooks.abcde.mango        |                  2 | 2026-08-19 10:00:01   | 2026-08-19 10:00:02
 processor-Dev03-5678       | xpto.webhooks.1234.cli2           |                  2 | 2026-08-19 10:00:01   | 2026-08-19 10:00:02
```

Com 1 worker, tudo aparece no mesmo worker_id. Com 2+, as filas sao distribuidas.

---

## 3. Ver todas as mensagens em ordem de processamento por customer

```sql
SELECT
    id,
    tenant_id,
    instance_id,
    customer_id,
    sequence,
    body,
    worker_id,
    processed_at
FROM processed_messages
ORDER BY customer_id, instance_id, processed_at;
```

Verifique visualmente que para cada customer_id a coluna `sequence` esta em ordem crescente (1, 2, 3, 4, 5...).

---

## 4. Confirmar que nenhum customer foi processado por dois workers ao mesmo tempo

```sql
SELECT
    customer_id,
    instance_id,
    COUNT(DISTINCT worker_id) AS worker_count,
    STRING_AGG(DISTINCT worker_id, ', ') AS workers
FROM processed_messages
WHERE customer_id IS NOT NULL AND customer_id != '__system__'
GROUP BY customer_id, instance_id
HAVING COUNT(DISTINCT worker_id) > 1;
```

**Resultado esperado: 0 linhas.** Se retornar alguma linha, significa que dois workers consumiram a mesma fila de customer (o consistent hashing falhou ou houve rebalanceamento durante o teste).

---

## 5. Tempo de processamento por mensagem

```sql
SELECT
    customer_id,
    instance_id,
    sequence,
    webhook_received_at,
    processed_at,
    processed_at - webhook_received_at::timestamptz AS latency
FROM processed_messages
WHERE customer_id IS NOT NULL AND customer_id != '__system__'
ORDER BY customer_id, instance_id, sequence;
```

A coluna `latency` mostra o tempo total desde o webhook chegar na API ate o processor terminar de processar.

---

## 6. Eventos de sistema (sem customer)

```sql
SELECT
    tenant_id,
    instance_id,
    event,
    worker_id,
    processed_at
FROM processed_messages
WHERE customer_id = '__system__'
ORDER BY processed_at;
```

---

## 7. Resumo geral

```sql
SELECT
    'Total de mensagens' AS metrica,
    COUNT(*)::text AS valor
FROM processed_messages

UNION ALL

SELECT
    'Customers unicos',
    COUNT(DISTINCT customer_id || '.' || instance_id)::text
FROM processed_messages
WHERE customer_id IS NOT NULL AND customer_id != '__system__'

UNION ALL

SELECT
    'Workers usados',
    COUNT(DISTINCT worker_id)::text
FROM processed_messages

UNION ALL

SELECT
    'Tenants',
    COUNT(DISTINCT tenant_id)::text
FROM processed_messages

UNION ALL

SELECT
    'Instancias',
    COUNT(DISTINCT instance_id)::text
FROM processed_messages

UNION ALL

SELECT
    'Mensagens fora de ordem',
    COALESCE(SUM(out_of_order_count), 0)::text
FROM v_order_check;
```

---

## 8. Limpar dados para rodar o teste de novo

```sql
TRUNCATE processed_messages RESTART IDENTITY;
```
