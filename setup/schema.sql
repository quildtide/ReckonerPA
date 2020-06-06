CREATE TABLE reckoner.smurfs (
    alt_player_type VARCHAR,
    alt_player_id VARCHAR,
    PRIMARY KEY(alt_player_type, alt_player_id),
    main_player_type VARCHAR,
    main_player_id VARCHAR
);
CREATE INDEX smurfs_alt_uberid ON reckoner.smurfs(alt_player_type, alt_player_id);

CREATE TABLE reckoner.gamefeed (
    lobbyid BIGINT PRIMARY KEY,
    system_name VARCHAR,
    system_info_gamefeed JSONB,
    mods VARCHAR [],
    bounty FLOAT,
    FFA BOOL DEFAULT FALSE,
    sandbox BOOL DEFAULT FALSE,
    obs_time BIGINT,
    QBE BOOL DEFAULT FALSE
);
CREATE INDEX gamefeed_lobbyid ON reckoner.gamefeed(lobbyid);
CREATE INDEX gamefeed_FFA ON reckoner.gamefeed(FFA);
CREATE INDEX gamefeed_sandbox ON reckoner.gamefeed(sandbox);
CREATE INDEX gamefeed_QBE ON reckoner.gamefeed(QBE);


CREATE TABLE reckoner.matches (
    match_id BIGINT PRIMARY KEY,
    lobbyid BIGINT,
    duration FLOAT,
    time_start INT NOT NULL,
    time_end INT,
    titans BOOL,
    living VARCHAR [],
    patch INT,
    ranked BOOL,
    tourney BOOL,
    mod_penalty FLOAT,
    mods VARCHAR [],
    mod_versions VARCHAR [],
    dimension_names VARCHAR [],
    dimension_values FLOAT [],
    sources VARCHAR [],
    system_name VARCHAR,
    system_info JSONB,
    system_info_gamefeed JSONB,
    server VARCHAR,
    uberids BIGINT [],
    all_dead BOOL,
    source_superstats BOOL DEFAULT FALSE,
    source_river BOOL DEFAULT FALSE,
    source_pastats BOOL DEFAULT FALSE,
    source_corrections BOOL DEFAULT FALSE,
    source_replayfeed BOOL DEFAULT FALSE,
    source_recorder BOOL DEFAULT FALSE,
    source_gamefeed BOOL DEFAULT FALSE,
    sandbox BOOL DEFAULT FALSE,
    bounty FLOAT
);
CREATE INDEX matches_match_id ON reckoner.matches(match_id);
CREATE INDEX matches_lobbyid ON reckoner.matches USING HASH(lobbyid);
CREATE INDEX matches_time_start ON reckoner.matches(time_start);
CREATE INDEX matches_time_end ON reckoner.matches(time_end);
CREATE INDEX matches_scored ON reckoner.matches(scored) WHERE scored IS NULL;

CREATE TABLE reckoner.teams (
    match_id BIGINT REFERENCES reckoner.matches(match_id) ON UPDATE CASCADE,
    team_num SMALLINT,
    PRIMARY KEY (match_id, team_num),
    win BOOL,
    shared BOOL,
    size SMALLINT
);
CREATE INDEX teams_index on reckoner.teams(match_id, team_num);

CREATE TABLE reckoner.armies (
    match_id BIGINT REFERENCES reckoner.matches(match_id),
    player_num SMALLINT,
    PRIMARY KEY(match_id, player_num),
    username VARCHAR,
    alpha FLOAT,
    beta FLOAT,
    player_type VARCHAR,
    player_id VARCHAR,
    faction CHAR(1),
    eco10 SMALLINT,
    team_num SMALLINT,
    commanders SMALLINT DEFAULT 1,
    FOREIGN KEY (match_id, team_num) REFERENCES reckoner.teams(match_id, team_num) ON UPDATE CASCADE
);
CREATE INDEX armies_player_id ON reckoner.armies USING HASH(player_system, player_id);
CREATE INDEX armies_index ON reckoner.armies(match_id, player_num);
CREATE INDEX armies_team_index ON reckoner.armies(match_id, team_num);

CREATE TABLE reckoner.mods (
    mod_id VARCHAR PRIMARY KEY,
    penalty FLOAT,
    parameters VARCHAR [],
    values FLOAT [],
    whitelist BOOL
);
CREATE INDEX mods_mod_id ON reckoner.mods USING HASH(mod_id);

CREATE TABLE reckoner.mod_aliases (
    alt_mod_id VARCHAR PRIMARY KEY,
    main_mod_id VARCHAR REFERENCES reckoner.mods(mod_id)
);
CREATE INDEX mod_aliases_alt_mod_id ON reckoner.mod_aliases USING HASH(alt_mod_id);

CREATE TABLE reckoner.name_history (
    player_type VARCHAR,
    player_id VARCHAR,
    username VARCHAR,
    PRIMARY KEY (player_type, player_id, username),
    times INT []
);
CREATE INDEX name_history_index ON reckoner.name_history(uberid, username);

CREATE TABLE reckoner.ubernames2 (
    uberid BIGINT PRIMARY KEY,
    ubername VARCHAR
);
CREATE INDEX ubernames_uberid ON reckoner.ubernames2(uberid);

CREATE OR REPLACE VIEW reckoner.match_aggregate AS
SELECT
    a.match_id,
    AVG(eco10 / 10.0) as eco_mean,
    VAR_SAMP(eco10 / 10.0) as eco_var,
    AVG(size) as team_size_mean,
    VAR_SAMP(size) as team_size_var,
    COUNT(DISTINCT(t.team_num)) as team_count,
    EVERY(alpha IS NOT NULL AND beta IS NOT NULL) as scored,

    (EVERY(player_id != '-1') AND EVERY(player_id != 'Idle') AND EVERY(player_id != 'Idle QBE')) as players_valid

FROM reckoner.armies AS a
INNER JOIN reckoner.teams AS t
ON (a.match_id, a.team_num) = (t.match_id, t.team_num)
GROUP BY a.match_id;


CREATE VIEW reckoner.merged_armies AS
SELECT 
    match_id,
    player_num,
    username,
    alpha,
    beta,
    faction,
    (eco10 / 10.0) as eco,
    team_num,
    commanders,
    main_player_type as player_type,
    main_player_id as player_id
FROM reckoner.armies
INNER JOIN reckoner.smurfs 
ON (alt_player_type, alt_player_id) = (player_type, player_id)
UNION ALL
SELECT 
    match_id,
    player_num,
    username,
    alpha,
    beta,
    faction,
    (eco10 / 10.0) as eco,
    team_num,
    commanders,
    player_type,
    player_id
FROM reckoner.armies
WHERE NOT EXISTS (
    SELECT * FROM reckoner.smurfs
    WHERE player_type = alt_player_type
    AND player_id = alt_player_id
);


CREATE OR REPLACE VIEW reckoner.matchrows AS
SELECT
    a.match_id,
    lobbyid,
    duration,
    time_start,
    time_end,
    titans,
    living,
    patch,
    ranked,
    tourney,
    mod_penalty,
    mods,
    mod_versions,
    dimension_names,
    dimension_values,
    system_name,
    system_info,
    server,
    uberids,
    all_dead,
    source_superstats,
    source_river,
    source_pastats,
    source_corrections,
    source_replayfeed,
    source_recorder,
    source_gamefeed,
    sandbox,

    a.team_num,
    win,
    shared,
    size as team_size,

    player_num,
    username,
    alpha,
    beta,
    faction,
    eco,
    commanders,
    player_type,
    player_id,

    d.eco_mean,
    eco_var,
    team_size_mean,
    team_size_var,
    team_count,
    scored,
    players_valid

FROM reckoner.teams a
INNER JOIN  merged_armies b
ON (a.match_id, a.team_num) = (b.match_id, b.team_num)
INNER JOIN reckoner.matches c
ON (a.match_id) = (c.match_id)
INNER JOIN reckoner.match_aggregate d
ON (c.match_id) = (d.match_id)
INNER JOIN (
    SELECT match_id, COUNT(*) as winner_count
    FROM reckoner.teams
    WHERE win
    GROUP BY match_id) e 
ON e.match_id = c.match_id
WHERE team_count > 1 AND players_valid AND (all_dead or e.winner_count = 1) AND NOT sandbox;

SELECT player_type, player_id, time_start as timestamp, match_id, team_num as team_id
FROM reckoner.matchrows
ORDER BY timestamp ASC;

SELECT player_type, 
    player_id, 
    time_start as timestamp, 
    match_id, 
    team_num as team_id,
    win,
    team_size,
    team_size_mean,
    team_size_var,
    team_count,
    match_id,
    eco,
    eco_mean,
    eco_var,
    all_dead,
    shared,
    titans
FROM reckoner.matchrows
WHERE scorable
ORDER BY timestamp ASC;


