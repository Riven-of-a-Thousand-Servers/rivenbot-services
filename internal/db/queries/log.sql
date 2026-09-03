-- name: CreateLogEntry :one
INSERT INTO ingestion_log (instance_id, source, status)
VALUES ($1, $2, $3)
ON CONFLICT (instance_id) DO NOTHING
RETURNING *;

-- name: UpdateLogEntryStatus :exec
UPDATE ingestion_log
SET
    status = $2,
    last_attempt_at = now(),
    error = $3
WHERE instance_id = $1;

-- name: ClaimLogEntryForProcessing :one
UPDATE ingestion_log
SET status = 'processing', last_attempt_at = now()
WHERE instance_id = $1 AND status = $2
RETURNING instance_id, status;
