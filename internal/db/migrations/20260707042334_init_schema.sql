-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS activity_name (
    id bigint PRIMARY KEY,
    activity_label text NOT NULL,
    is_active boolean NOT NULL,
    release_date timestamp(0) with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_difficulty (
    id bigint PRIMARY KEY,
    activity_label text NOT NULL
);

CREATE TABLE IF NOT EXISTS activity (
    activity_hash bigint PRIMARY KEY,
    activity_label text NOT NULL,
    name_id bigint NOT NULL,
    difficulty_id bigint NOT NULL,
    is_worlds_first boolean,
    CONSTRAINT activity_name_fk FOREIGN KEY (name_id) REFERENCES activity_name (id),
    CONSTRAINT activity_difficulty_fk FOREIGN KEY (difficulty_id) REFERENCES activity_difficulty (id)
);

CREATE TABLE IF NOT EXISTS weapon (
    weapon_hash bigint PRIMARY KEY,
    icon_url text NOT NULL,
    weapon_name text NOT NULL,
    equipment_slot text NOT NULL,
    damage_type text NOT NULL
);

CREATE TABLE IF NOT EXISTS instance (
    id bigint PRIMARY KEY,
    activity_hash bigint NOT NULL,
    is_fresh boolean NOT NULL,
    flawless boolean NOT NULL,
    completed boolean NOT NULL,
    player_count int NOT NULL,
    duration_seconds int NOT NULL,
    end_time timestamp NOT NULL,
    start_time timestamp NOT NULL,
    created_at timestamp NOT NULL DEFAULT now(),
    CONSTRAINT instance_hash_fk FOREIGN KEY (activity_hash) REFERENCES activity (activity_hash)
);

CREATE INDEX IF NOT EXISTS instance_activity_hash_idx ON instance (activity_hash);

CREATE TABLE IF NOT EXISTS pgcr (
    instance_id bigint PRIMARY KEY,
    blob bytea NOT NULL,
    created_at timestamp NOT NULL DEFAULT now(),
    CONSTRAINT instance_id_fk FOREIGN KEY (instance_id) REFERENCES instance (id)
);

CREATE TABLE IF NOT EXISTS destiny_player (
    membership_id bigint PRIMARY KEY,
    membership_type int NOT NULL,
    icon_path text,
    display_name text,
    global_display_name text,
    global_display_name_code int,
    total_clears int NOT NULL DEFAULT 0,
    total_full_clears int NOT NULL DEFAULT 0,
    is_public boolean DEFAULT FALSE,
    last_crawled timestamp NOT NULL,
    last_seen timestamp,
    created_at timestamp NOT NULL DEFAULT now()
);

CREATE INDEX destiny_player_display_name_idx ON destiny_player (display_name);

CREATE TABLE IF NOT EXISTS instance_player (
    instance_id bigint NOT NULL,
    membership_id bigint NOT NULL,
    completed boolean DEFAULT FALSE,
    time_played_seconds int NOT NULL,
    created_at timestamp NOT NULL DEFAULT now(),
    CONSTRAINT instance_player_pk PRIMARY KEY (instance_id, membership_id)
);

CREATE INDEX IF NOT EXISTS instance_player_membership_id_idx ON instance_player (
    membership_id
);

CREATE TABLE IF NOT EXISTS instance_character (
    instance_id bigint NOT NULL,
    membership_id bigint NOT NULL,
    character_id bigint NOT NULL,
    class_hash bigint NOT NULL,
    emblem_hash bigint NOT NULL,
    completed boolean NOT NULL,
    kills int NOT NULL,
    deaths int NOT NULL,
    assists int NOT NULL,
    kda decimal NOT NULL,
    kdr decimal NOT NULL,
    super_kills int NOT NULL,
    melee_kills int NOT NULL,
    grenade_kills int NOT NULL,
    efficiency int NOT NULL,
    time_played_seconds int NOT NULL,
    CONSTRAINT instance_character_pk PRIMARY KEY (instance_id, membership_id, character_id),
    CONSTRAINT instance_character_instance_fk FOREIGN KEY (instance_id) REFERENCES instance (id),
    CONSTRAINT instance_character_destiny_player_fk FOREIGN KEY (membership_id) REFERENCES destiny_player (membership_id)
);

CREATE INDEX IF NOT EXISTS instance_character_membership_id_idx ON instance_character (
    membership_id
);

CREATE TABLE IF NOT EXISTS instance_character_weapon (
    instance_id bigint NOT NULL,
    player_membership_id bigint NOT NULL,
    player_character_id bigint NOT NULL,
    weapon_id bigint NOT NULL,
    kills int NOT NULL DEFAULT 0,
    precision_kills int NOT NULL DEFAULT 0,
    precision_ratio decimal NOT NULL DEFAULT 0.0,
    CONSTRAINT instance_character_weapon_pk PRIMARY KEY (instance_id, player_membership_id, player_character_id, weapon_id),
    CONSTRAINT instance_character_weapon_instance_fk FOREIGN KEY (instance_id) REFERENCES instance (id),
    CONSTRAINT instance_character_weapon_player_fk FOREIGN KEY (player_membership_id) REFERENCES destiny_player (membership_id),
    CONSTRAINT instance_character_weapon_character_fk FOREIGN KEY (instance_id, player_membership_id, player_character_id) REFERENCES instance_character (instance_id, membership_id, character_id),
    CONSTRAINT instance_character_weapon_weapon_fk FOREIGN KEY (weapon_id) REFERENCES weapon (weapon_hash)
);

CREATE INDEX IF NOT EXISTS instance_character_weapon_weapon_idx ON instance_character_weapon (
    weapon_id
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    instance_id bigint PRIMARY KEY,
    source text NOT NULL,             -- e.g. 'dataset', 'crawler'
    -- 'success' | 'error' | 'processing'
    status text NOT NULL DEFAULT 'processing',
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_attempt_at timestamptz NOT NULL DEFAULT now(),
    attempt_count int NOT NULL DEFAULT 1,
    error text
);

-- +goose StatementEnd
-- +goose Down
DROP TABLE IF EXISTS ingestion_log;

DROP TABLE IF EXISTS instance_character_weapon;

DROP TABLE IF EXISTS weapon;

DROP TABLE IF EXISTS instance_character;

DROP TABLE IF EXISTS instance_player;

DROP INDEX CONCURRENTLY IF EXISTS destiny_player_display_name_idx;

DROP TABLE IF EXISTS pgcr;

DROP TABLE IF EXISTS destiny_player;

DROP TABLE IF EXISTS instance;

DROP TABLE IF EXISTS activity;

DROP TABLE IF EXISTS activity_difficulty;

DROP TABLE IF EXISTS activity_name;
