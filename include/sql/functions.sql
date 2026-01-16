-- Функции stg -> dds

CREATE OR REPLACE FUNCTION dds.load_dim_players()
RETURNS void LANGUAGE SQL AS
$$
    WITH source_data AS (
        SELECT * FROM stg.players
    ),
    deactivating_old AS (
        UPDATE dds.dim_players AS p
        SET 
            active_to = s.clan_war_time, 
            is_current = FALSE
        FROM source_data AS s
        WHERE p.player_tag = s.player_tag 
        AND p.is_current = TRUE
        AND (p.townhall_level <> s.townhall_level OR p.player_name <> s.player_name OR p.clan_tag <> s.clan_tag)
        RETURNING p.player_tag
    )
    INSERT INTO dds.dim_players (player_tag, player_name, clan_tag, townhall_level, active_from)
    SELECT
        s.player_tag, s.player_name, s.clan_tag, s.townhall_level, s.clan_war_time
    FROM source_data AS s LEFT JOIN dds.dim_players as p ON
    p.player_tag = s.player_tag AND p.is_current = TRUE
    WHERE p.player_tag IS NULL
$$;

CREATE OR REPLACE FUNCTION dds.load_dim_clans()
RETURNS void LANGUAGE SQL AS
$$
    WITH source_data AS (
        SELECT * FROM stg.clans
    ),
    deactivating_old AS (
        UPDATE dds.dim_clans AS c
        SET 
            active_to = s.clan_war_time, 
            is_current = FALSE
        FROM source_data AS s
        WHERE c.clan_tag = s.clan_tag 
        AND c.is_current = TRUE
        AND (c.clan_name <> s.clan_name OR c.clan_level <> s.clan_level)
        RETURNING c.clan_tag
    )
    INSERT INTO dds.dim_clans (clan_tag, clan_name, clan_level, active_from)
    SELECT
        s.clan_tag, s.clan_name, s.clan_level, s.clan_war_time
    FROM source_data AS s LEFT JOIN dds.dim_clans as c ON
    c.clan_tag = s.clan_tag AND c.is_current = TRUE
    WHERE c.clan_tag IS NULL
$$;

CREATE OR REPLACE FUNCTION dds.load_fact_war()
RETURNS void LANGUAGE SQL AS
$$
    WITH source_data AS (
        SELECT * FROM stg.wars
    )
    INSERT INTO dds.fact_wars (start_time, end_time, team_size, allay_clan_key, opponent_clan_key,
                               allay_attacks, allay_stars, allay_destruction_percentage, opponent_attacks,
                               opponent_stars, opponent_destruction_percentage, is_allay_winner, is_draw)
    SELECT
        s.start_time,
        s.end_time,
        s.team_size,
        (SELECT clan_key
         FROM dds.dim_clans 
         WHERE s.allay_clan_tag = clan_tag AND s.start_time BETWEEN active_from AND active_to 
         LIMIT 1) AS allay_clan_key,
        (SELECT clan_key 
         FROM dds.dim_clans 
         WHERE s.opponent_clan_tag = clan_tag AND s.start_time BETWEEN active_from AND active_to
         LIMIT 1) AS opponent_clan_key,
        s.allay_attacks,
        s.allay_stars,
        s.allay_destruction_percentage,
        s.opponent_attacks,
        s.opponent_stars,
        s.opponent_destruction_percentage,
        s.is_allay_winner,
        s.is_draw
    FROM source_data AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.fact_wars f
        WHERE f.allay_clan_key = allay_clan_key
          AND f.opponent_clan_key = opponent_clan_key
          AND f.start_time = s.start_time
    )
$$;

CREATE OR REPLACE FUNCTION dds.load_fact_attacks()
RETURNS void LANGUAGE SQL AS
$$
    WITH source_data AS (
        SELECT * FROM stg.attacks
    )
    INSERT INTO dds.fact_attacks (war_id, player_key, opponent_player_key, stars, percentage,
                                  attack_duration, attack_order)
    SELECT
        (SELECT war_id FROM dds.fact_wars WHERE start_time = s.war_start_time LIMIT 1) as war_id,
        (SELECT player_key
         FROM dds.dim_players 
         WHERE s.player_tag = player_tag AND s.war_start_time BETWEEN active_from AND active_to 
         LIMIT 1) as player_key,
        (SELECT player_key
         FROM dds.dim_players 
         WHERE s.opponent_player_tag = player_tag AND s.war_start_time BETWEEN active_from AND active_to 
         LIMIT 1) as opponent_player_key,
        s.stars,
        s.percentage,
        s.attack_duration,
        s.attack_order
    FROM source_data AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.fact_attacks fa
        WHERE fa.war_id = war_id 
          AND fa.player_key = player_key
          AND fa.opponent_player_key = opponent_player_key
          AND fa.attack_order = s.attack_order
    )
$$;

CREATE OR REPLACE FUNCTION dds.load_dim_war_participants()
RETURNS void LANGUAGE SQL AS
$$
    WITH source_data AS (
        SELECT * FROM stg.war_participants 
    )
    INSERT INTO dds.dim_war_participants (war_id, player_key, map_position)
    SELECT
        (SELECT war_id FROM dds.fact_wars WHERE start_time = s.war_start_time LIMIT 1) as war_id,
        (SELECT player_key
         FROM dds.dim_players 
         WHERE s.player_tag = player_tag AND s.war_start_time BETWEEN active_from AND active_to 
         LIMIT 1) as player_key,
        s.map_position
    FROM source_data AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM dds.dim_war_participants wp
        WHERE wp.war_id = war_id 
          AND wp.player_key = player_key
    )
$$;