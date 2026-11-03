-- name: CreateInstance :exec
INSERT INTO instance (
    id,
    activity_hash,
    is_fresh,
    flawless,
    completed,
    player_count,
    duration_seconds,
    end_time,
    start_time
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
) ON CONFLICT (id) DO NOTHING;
