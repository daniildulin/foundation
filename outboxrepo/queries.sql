-- name: CreateOutboxEvent :exec
INSERT INTO foundation_outbox_events (topic, key, payload, headers, created_at)
VALUES ($1, $2, $3, $4, NOW());

-- Locks the batch it returns and skips rows another courier already holds, so
-- that several courier replicas can run without publishing the same events
-- twice.
-- name: ListOutboxEvents :many
SELECT * FROM foundation_outbox_events
ORDER BY id ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- Deletes exactly the rows that were published. Deleting everything up to a
-- maximum id would also remove rows a concurrent courier had locked but not yet
-- committed — and if that courier then rolled back, those events would be gone
-- without ever having been published.
-- name: DeleteOutboxEvents :exec
DELETE FROM foundation_outbox_events WHERE id = ANY($1::bigint[]);
