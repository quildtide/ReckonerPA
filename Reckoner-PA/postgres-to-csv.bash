psql -c \
"COPY(\
SELECT player_type, \
    player_id, \
    time_start as timestamp, \
    match_id, \
    team_num as team_id,\
    win,\
    team_size,\
    team_size_mean,\
    team_size_var,\
    team_count,\
    match_id,\
    eco,\
    eco_mean,\
    eco_var,\
    all_dead,\
    shared,\
    titans, \
    ranked, \
    tourney \
FROM reckoner.matchrows \
ORDER BY timestamp ASC) \
TO STDOUT \
WITH CSV HEADER;"\
> pa_output.csv