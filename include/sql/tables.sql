-- staging слой
CREATE SCHEMA stg;
-- Таблица clans
CREATE TABLE stg.clans (
    clan_tag VARCHAR(12) NULL,
    clan_name VARCHAR(50) NULL,
    clan_level INT NULL
);

-- Таблица players
CREATE TABLE stg.players (
    player_tag VARCHAR(12) NULL,
    player_name VARCHAR(50) NULL,
    clan_tag VARCHAR(12),
    townhall_level SMALLINT NULL
);

-- Таблица wars
CREATE TABLE stg.wars (
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    team_size INT NULL,
    allay_clan_tag VARCHAR(12) NULL,
    opponent_clan_tag VARCHAR(12) NULL,
    
    allay_attacks INT NULL,
    allay_stars INT NULL,
    allay_destruction_percentage DECIMAL(9,6) NULL,
    
    opponent_attacks INT NULL,
    opponent_stars INT NULL,
    opponent_destruction_percentage DECIMAL(9,6) NULL,
    
    is_allay_winner BOOLEAN,
    is_draw BOOLEAN DEFAULT FALSE,
    is_war_ended BOOLEAN DEFAULT FALSE
);

-- Таблица attacks
CREATE TABLE stg.attacks (
    player_tag VARCHAR(12) NOT NULL,
    opponent_player_tag VARCHAR(12) NOT NULL,
    
    stars SMALLINT NOT NULL,
    percentage SMALLINT NOT NULL,
    attack_duration SMALLINT NOT NULL,
    
    attack_order SMALLINT,
    war_start_time TIMESTAMP NULL
);

-- Таблица war_participants
CREATE TABLE stg.war_participants (
    player_tag VARCHAR(12) NOT NULL,
    map_position SMALLINT NOT NULL,
    war_start_time TIMESTAMP NULL
);

-- dds слой
CREATE SCHEMA dds;
-- Таблица dim_clans
CREATE TABLE dds.dim_clans (
    clan_key SERIAL PRIMARY KEY,
    clan_tag VARCHAR(12) NOT NULL,
    clan_name VARCHAR(50) NOT NULL,
    clan_level INT NOT NULL,
    active_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_clans_current_boolean ON dds.dim_clans (is_current, clan_tag);
CREATE INDEX idx_dim_clans_active_range ON dds.dim_clans (active_from, active_to, clan_tag);
CREATE INDEX idx_dim_clans_current_clans ON dds.dim_clans (clan_tag) WHERE is_current = TRUE;

-- Таблица dim_players
CREATE TABLE dds.dim_players (
    player_key SERIAL PRIMARY KEY,
    player_tag VARCHAR(12) NOT NULL,
    player_name VARCHAR(50) NOT NULL,
    clan_tag VARCHAR(12),
    townhall_level SMALLINT NOT NULL,
    active_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_players_current_boolean ON dds.dim_players (is_current, player_tag);
CREATE INDEX idx_dim_players_active_range ON dds.dim_players (active_from, active_to, player_tag);
CREATE INDEX idx_dim_players_current_players ON dds.dim_players (player_tag) WHERE is_current = TRUE;

-- Таблица fact_wars
CREATE TABLE dds.fact_wars (
    war_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    team_size INT NOT NULL,
    allay_clan_key INT NOT NULL,
    opponent_clan_key INT NOT NULL,
    
    allay_attacks INT NOT NULL,
    allay_stars INT NOT NULL,
    allay_destruction_percentage DECIMAL(9,6) NOT NULL,
    
    opponent_attacks INT NOT NULL,
    opponent_stars INT NOT NULL,
    opponent_destruction_percentage DECIMAL(9,6) NOT NULL,
    
    is_allay_winner BOOLEAN,
    is_draw BOOLEAN DEFAULT FALSE,
    
    war_date DATE GENERATED ALWAYS AS (DATE(start_time)) STORED,
    war_duration_minutes INT GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (end_time - start_time)) / 60
    ) STORED,
    war_month DATE GENERATED ALWAYS AS (DATE_TRUNC('month', start_time)) STORED,
    total_attacks INT GENERATED ALWAYS AS (allay_attacks + opponent_attacks) STORED,
    total_stars INT GENERATED ALWAYS AS (allay_stars + opponent_stars) STORED,
    
    CONSTRAINT chk_team_size CHECK (team_size > 0),
    CONSTRAINT chk_time_order CHECK (end_time > start_time),
    CONSTRAINT chk_allay_percentage CHECK (allay_destruction_percentage BETWEEN 0 AND 100),
    CONSTRAINT chk_opponent_percentage CHECK (opponent_destruction_percentage BETWEEN 0 AND 100),
    CONSTRAINT chk_allay_stars CHECK (allay_stars BETWEEN 0 AND team_size * 3),
    CONSTRAINT chk_opponent_stars CHECK (opponent_stars BETWEEN 0 AND team_size * 3),
    CONSTRAINT chk_war_status CHECK (
        (is_allay_winner IS NULL AND NOT is_draw) OR 
        (is_allay_winner IS NOT NULL AND is_draw = FALSE) OR
        (is_allay_winner IS NULL AND is_draw = TRUE)
    )
);

CREATE INDEX idx_fact_wars_date ON dds.fact_wars (war_date);
CREATE INDEX idx_fact_wars_battling_clans ON dds.fact_wars (allay_clan_key, opponent_clan_key);
CREATE INDEX idx_fact_wars_time_range ON dds.fact_wars (start_time, end_time);
CREATE INDEX idx_fact_wars_allay_clan ON dds.fact_wars (allay_clan_key);
CREATE INDEX idx_fact_wars_opponent_clan ON dds.fact_wars (opponent_clan_key);

-- Таблица fact_attacks
CREATE TABLE dds.fact_attacks (
    attack_id SERIAL PRIMARY KEY,
    war_id INT NOT NULL,
    
    player_key INT NOT NULL,
    opponent_player_key INT NOT NULL,
    
    stars SMALLINT NOT NULL,
    percentage SMALLINT NOT NULL,
    attack_duration SMALLINT NOT NULL,
    
    attack_order SMALLINT,
    
    is_triple BOOLEAN GENERATED ALWAYS AS (stars = 3) STORED,
    is_failed BOOLEAN GENERATED ALWAYS AS (stars = 0) STORED,
    
    CONSTRAINT chk_stars_range CHECK (stars BETWEEN 0 AND 3),
    CONSTRAINT chk_percentage_range CHECK (percentage BETWEEN 0 AND 100),
    CONSTRAINT chk_attack_duration CHECK (attack_duration > 0)
);

CREATE INDEX idx_fact_attacks_war_id ON dds.fact_attacks (war_id);
CREATE INDEX idx_fact_attacks_composite ON dds.fact_attacks (war_id, player_key);

CREATE TABLE dds.dim_war_participants (
    war_id INT NOT NULL,
    player_key INT NOT NULL,
    map_position SMALLINT NOT NULL,
    
    PRIMARY KEY (war_id, player_key)
);

CREATE INDEX idx_war_participants_player ON dds.dim_war_participants(player_key);

