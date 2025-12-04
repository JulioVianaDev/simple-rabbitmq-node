-- Create table to track consumed messages
CREATE TABLE IF NOT EXISTS consumed_messages (
    id SERIAL PRIMARY KEY,
    instance_id VARCHAR(255) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    message_content TEXT,
    message_sequence INTEGER,
    consumed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    worker_id VARCHAR(255)
);

-- Create index for efficient querying by instance
CREATE INDEX IF NOT EXISTS idx_consumed_messages_instance_id ON consumed_messages(instance_id);
CREATE INDEX IF NOT EXISTS idx_consumed_messages_queue_name ON consumed_messages(queue_name);
CREATE INDEX IF NOT EXISTS idx_consumed_messages_consumed_at ON consumed_messages(consumed_at);

-- Create index for ordering verification
CREATE INDEX IF NOT EXISTS idx_consumed_messages_instance_sequence ON consumed_messages(instance_id, message_sequence);

-- Function to check message ordering per instance
CREATE OR REPLACE FUNCTION check_message_ordering(instance VARCHAR)
RETURNS TABLE(
    queue_name VARCHAR,
    total_messages BIGINT,
    ordered_messages BIGINT,
    out_of_order_messages BIGINT,
    is_ordered BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    WITH message_order AS (
        SELECT 
            queue_name,
            message_sequence,
            consumed_at,
            LAG(consumed_at) OVER (PARTITION BY queue_name ORDER BY message_sequence) as prev_consumed_at,
            LAG(message_sequence) OVER (PARTITION BY queue_name ORDER BY message_sequence) as prev_sequence
        FROM consumed_messages
        WHERE instance_id = instance
    ),
    order_check AS (
        SELECT 
            queue_name,
            COUNT(*) as total,
            SUM(CASE 
                WHEN prev_sequence IS NULL OR (prev_sequence < message_sequence AND prev_consumed_at <= consumed_at) 
                THEN 1 
                ELSE 0 
            END) as ordered,
            SUM(CASE 
                WHEN prev_sequence IS NOT NULL AND (prev_sequence >= message_sequence OR prev_consumed_at > consumed_at)
                THEN 1 
                ELSE 0 
            END) as out_of_order
        FROM message_order
        GROUP BY queue_name
    )
    SELECT 
        queue_name::VARCHAR,
        total,
        ordered,
        out_of_order,
        (out_of_order = 0)::BOOLEAN as is_ordered
    FROM order_check;
END;
$$ LANGUAGE plpgsql;

