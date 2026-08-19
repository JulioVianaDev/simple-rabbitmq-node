-- Tenant/instance mapping
CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS instances (
    instance_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL REFERENCES tenants(id)
);

-- Customer contacts (used to identify customerId from webhook data)
CREATE TABLE IF NOT EXISTS contacts (
    id VARCHAR(50) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL REFERENCES tenants(id),
    instance_id VARCHAR(255) NOT NULL REFERENCES instances(instance_id),
    phone VARCHAR(50),
    name VARCHAR(255),
    PRIMARY KEY (id, tenant_id, instance_id)
);

CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone, instance_id);

-- Seed data
INSERT INTO tenants (id, name) VALUES
    ('xpto', 'Empresa XPTO'),
    ('tomat', 'Empresa Tomat')
ON CONFLICT DO NOTHING;

INSERT INTO instances (instance_id, tenant_id) VALUES
    ('1234', 'xpto'),
    ('1231', 'xpto'),
    ('abcde', 'tomat'),
    ('goiaba', 'tomat')
ON CONFLICT DO NOTHING;

INSERT INTO contacts (id, tenant_id, instance_id, phone, name) VALUES
    ('bat1', 'xpto', '1234', '5511999990001', 'Cliente Bat1'),
    ('cli2', 'xpto', '1234', '5511999990002', 'Cliente Cli2'),
    ('cli3', 'xpto', '1231', '5511999990003', 'Cliente Cli3'),
    ('mango', 'tomat', 'abcde', '5511999990004', 'Cliente Mango'),
    ('kiwi', 'tomat', 'abcde', '5511999990005', 'Cliente Kiwi'),
    ('uva', 'tomat', 'goiaba', '5511999990006', 'Cliente Uva')
ON CONFLICT DO NOTHING;

-- Message processing log (for testing order and worker assignment)
CREATE TABLE IF NOT EXISTS processed_messages (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL,
    instance_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255),
    queue_name VARCHAR(500) NOT NULL,
    event VARCHAR(100),
    body TEXT,
    sequence INTEGER,
    webhook_received_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    worker_id VARCHAR(255) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processed_tenant ON processed_messages(tenant_id);
CREATE INDEX IF NOT EXISTS idx_processed_customer ON processed_messages(customer_id, instance_id);
CREATE INDEX IF NOT EXISTS idx_processed_queue ON processed_messages(queue_name);
CREATE INDEX IF NOT EXISTS idx_processed_worker ON processed_messages(worker_id);

-- View: check ordering per customer (sequence should always increase)
CREATE OR REPLACE VIEW v_order_check AS
SELECT
    tenant_id,
    instance_id,
    customer_id,
    COUNT(*) AS total_messages,
    BOOL_AND(
        seq_ok
    ) AS all_in_order,
    COUNT(*) FILTER (WHERE NOT seq_ok) AS out_of_order_count
FROM (
    SELECT
        tenant_id,
        instance_id,
        customer_id,
        sequence,
        processed_at,
        COALESCE(
            sequence > LAG(sequence) OVER (
                PARTITION BY customer_id, instance_id
                ORDER BY processed_at
            ),
            true
        ) AS seq_ok
    FROM processed_messages
    WHERE customer_id IS NOT NULL AND customer_id != '__system__'
) sub
GROUP BY tenant_id, instance_id, customer_id;

-- View: which worker processed which queues
CREATE OR REPLACE VIEW v_worker_distribution AS
SELECT
    worker_id,
    queue_name,
    COUNT(*) AS messages_processed,
    MIN(processed_at) AS first_at,
    MAX(processed_at) AS last_at
FROM processed_messages
GROUP BY worker_id, queue_name
ORDER BY worker_id, queue_name;
