import CSV
import JSON
import LibPQ
import Base.Filesystem


include("reckoner_common.jl")

function import_local_bot_matches(conn)
    main_dir::String = "manual_data_sources/local_bot_matches/"
    match_dir::String = main_dir * "replay_logs/"

    function handle_record(filename)
        Set(parse.(Int32, readlines(main_dir * filename)))
    end

    second_wave = handle_record("2w_record.csv")
    qbe = handle_record("qbe_record.csv")
    team = handle_record("team_record.csv")

    function process_bot_match(filename)
        match = JSON.parsefile(match_dir * filename)

        time_start = Int32(match["save"]["utc_timestamp"])

        armies = match["armies"]

        is_2w = time_start in second_wave
        is_qbe = time_start in qbe
        is_team = time_start in qbe

        player_count = length(armies)

        team_size = if is_team player_count ÷ 2 else player_count end
        team_count = Int32(player_count / team_size)

        match_id = match_id_generation(time_start, ["",""], "local", 0)

        titans = "PAExpansion1" in match["required_content"]
        
        winner::Vector{Bool} = zeros(Bool, team_count)
        for army in armies
            if !army["defeated"]
                winner[floor(Int, army["index"] / team_size) + 1] = true
                break
            end
        end

        LibPQ.execute(conn, "   
            INSERT INTO reckoner.matches (
                match_id, time_start,
                source_local_bot, is_2w, 
                bounty, titans, 
                all_dead
            )
            VALUES (
                $match_id, $time_start,
                TRUE, $is_2w,
                0.0, $titans,
                $(sum(winner) == 0)
            )
            ON CONFLICT DO NOTHING;
        ")

        for team_num in 1:team_count
            LibPQ.execute(conn, "
                INSERT INTO reckoner.teams (
                    match_id, team_num,
                    win, shared, size
                )
                VALUES (
                    $match_id, $team_num,
                    $(winner[team_num]), FALSE, $team_size
                )
                ON CONFLICT DO NOTHING;
            ")
        end

        for army in armies
            playertype = "aiDiff"
            playerid = army["personality"]["name"]

            if is_qbe
                player_id = playerid * " QBE"
            end

            team_num = floor(Int, army["index"] / team_size) + 1

            LibPQ.execute(conn, "
                INSERT INTO reckoner.armies (
                    match_id, team_num, player_num, 
                    player_type, player_id
                )
                VALUES (
                    $match_id, $team_num, $(army["index"] + 1),
                    '$playertype', '$playerid'
                )
                ON CONFLICT DO NOTHING;
            ")
        end
    end

    LibPQ.execute(conn, "BEGIN;")

    for (i, j, k) in Filesystem.walkdir(match_dir)
        for m in k
            process_bot_match(m)
        end
    end

    LibPQ.execute(conn, "COMMIT;")
end


conn = LibPQ.Connection("dbname=reckoner user=reckoner")
import_local_bot_matches(conn)