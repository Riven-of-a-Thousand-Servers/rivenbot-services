-- name: CreateDestinyPlayer :exec
INSERT INTO destiny_player (
    membership_id,
    membership_type,
    icon_path,
    display_name,
    global_display_name,
    global_display_name_code,
    total_clears,
    total_full_clears,
    is_public,
    last_crawled,
    last_seen
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    now(),
    now()
) ON CONFLICT (membership_id)
DO UPDATE
    SET
        global_display_name = excluded.global_display_name,
        global_display_name_code = excluded.global_display_name_code,
        display_name = coalesce(excluded.display_name, destiny_player.display_name),
        icon_path = coalesce(excluded.icon_path, destiny_player.icon_path)
    WHERE
    excluded.global_display_name IS NOT NULL AND destiny_player.global_display_name IS NULL;

-- name: IncrementPlayerCounts :exec
UPDATE destiny_player
SET
    total_clears = total_clears + CASE WHEN $2::boolean THEN 1 ELSE 0 END,
    total_full_clears = total_full_clears + CASE WHEN $3::boolean THEN 1 ELSE 0 END
WHERE membership_id = $1;
