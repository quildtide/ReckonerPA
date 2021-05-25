import LibPQ
import JSON

include("replayfeed_common.jl")
include("name_history.jl")

function merge_legacy_namehist(conn)
    namehist::NameHist = NameHist()

    n::Int64 = 0

    for i in LibPQ.execute(conn, "SELECT COUNT(*) FROM reckoner.namehist;")
        n = i.count
    end

    interval::Int64 = 2500
    index::Int64 = 58000

    stmt_namehist_select = LibPQ.prepare(conn, "
        SELECT * FROM reckoner.namehist
        ORDER BY (uberid, name)
        LIMIT \$1
        OFFSET \$2;"
    )
    while index < (n + interval)
        for i in stmt_namehist_select((interval, index))
            key::Key = ("pa inc", i.uberid, i.name)
            namehist[key] = SortedSet{Timestamp}(i.timestamp)
        end

        name_hist_to_postgres(namehist)

        index += interval
    end

end

function fix_aidiff(input::String)::String
    if length(input) > 5
        if input[1:5] == "!LOC:"
            return input[6:end]
        end
    end
    input
end

function split_stringarr(input::String)::Vector{String}
    split(input[2:end-1], ',', keepempty = false)
end

function process_matches(matches, conn, timestamp_1::Timestamp, timestamp_2::Timestamp; match_time_range::Int64 = 1000)
    stmt_find_ai_victor = LibPQ.prepare(conn, "
        SELECT DISTINCT(team_num)
        FROM reckoner.armies
        WHERE player_id = \$1
        OR player_id = 'AI-Unknown';"
    )

    stmt_find_matched_player = LibPQ.prepare(conn, "
        SELECT username, player_id
        FROM reckoner.armies
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range)
        AND player_type IN ('superstats', 'pa inc');"
    )

    stmt_updt_playerid_human = LibPQ.prepare(conn, "   
        UPDATE reckoner.armies
        SET player_type = 'pa inc',
        player_id = \$2
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range)
        AND username = \$3;"
    )

    stmt_updt_playerid_ai = LibPQ.prepare(conn, "
        UPDATE reckoner.armies
        SET player_id = \$2
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range)
        AND player_type = 'aiDiff'
        AND player_id = 'AI-Unknown';"
    )

    stmt_updt_source_recorder_all_dead = LibPQ.prepare(conn, "   
        UPDATE reckoner.matches
        SET source_recorder = TRUE,
        all_dead = \$2
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range);"
    )

    stmt_updt_source_recorder = LibPQ.prepare(conn, "   
        UPDATE reckoner.matches
        SET source_recorder = TRUE
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range);"
    )

    stmt_update_team_victory = LibPQ.prepare(conn, "
        UPDATE reckoner.teams
        SET win = \$3
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range)
        AND team_num = \$2;"
    )

    stmt_insert_match = LibPQ.prepare(conn, "
        INSERT INTO reckoner.matches (
            match_id, lobbyid, time_start,
            duration, titans, all_dead, 
            patch, source_recorder, server
        ) VALUES (
            \$1, \$2, \$3, \$4, \$5,
            \$6, \$7, TRUE, 'pa inc'
        );"
    )

    stmt_insert_team = LibPQ.prepare(conn, "
        INSERT INTO reckoner.teams (
            match_id, team_num, win,
            shared, size
        ) VALUES (
            \$1, \$2, \$3, \$4, \$5
        );"
    )

    stmt_insert_army = LibPQ.prepare(conn, "
        INSERT INTO reckoner.armies (
            match_id, player_num, team_num,
            username, player_type, player_id,
            eco10, commanders
        ) VALUES (
            \$1, \$2, \$3, \$4, 
            \$5, \$6, \$7, \$8
        );"
    )

    stmt_push_gamefeed_to_matches, stmt_get_namehist, stmt_get_ubernames = gen_replayfeed_common_stmts(conn)

    update_full_count = 0
    update_id_count = 0
    insert_count = 0
    n_queries = 0

    function update_recorder_match(match, armies, is_qbe::Bool, update::Dict{Tuple{LobbyId, Username}, PastComposite})
        update_full_count += 1
    
        lobbyid::LobbyId = lobbyid_transform(match.lobbyid)
        
        timestamp::Timestamp = match.timestamp + (4 * 3600)
    
        match_id::MatchId = generate_match_id(timestamp, lobbyid)
        
        usernames = Vector{Username}()
        known_uberids = Dict{Username, UberId}()
    
        all_dead::Bool = true
        team_victory = Dict{TeamNum, Bool}()
        ai_victor::String = "hello world"
        known_victor::Bool = false
    
        ai_types::Set{String} = Set{String}()
        
        for i in armies
            alive = !(i["defeated"])
            if alive
                all_dead = false
            end
    
            if !i["ai"]
                usernames = vcat(usernames, i["players"])
                if length(i["players"]) > 0
                    curr_username = i["players"][1]
                    if (lobbyid, curr_username) in keys(update)
                        team_num = update[(lobbyid, curr_username)].team_num
                    else # abort
                        return nothing
                    end
    
                    if !(team_num in keys(team_victory))
                        team_victory[team_num] = alive
                    elseif !(team_victory[team_num])
                        team_victory[team_num] = alive
                    end
                    if alive
                        known_victor = true
                    end
                end
                
    
                for j in i["players"]
                    push!(known_uberids, j => update[(lobbyid, j)].uberid)
                end
            else
                ai_type =   if length(i["players"]) > 0
                                fix_aidiff(i["players"][1])
                            else
                                "Unknown AI Recorder"
                            end
                if is_qbe
                    ai_type = ai_type * " QBE"
                end
    
                if alive
                    ai_victor = ai_type
                end
                push!(ai_types, ai_type)
            end
        end
    
        if !known_victor && !all_dead
            res = stmt_find_ai_victor(ai_victor) |> Tables.rowtable
    
            if length(res) == 1
                team_victory[res[1].team_num] = true
            end
        end
    
        if "-1" in values(known_uberids)
            uberids = split_stringarr(match.uberids)
            unpaired_uberids::Vector{UberId} = setdiff(uberids, values(known_uberids))
            unpaired_usernames::Vector{Username} = usernames[[known_uberids[i] != "-1" for i in usernames]]
    
            new_pairs = pair_uberids(unpaired_usernames, unpaired_uberids, timestamp, stmt_get_namehist, stmt_get_ubernames)
    
            for i in unpaired_usernames
                stmt_updt_playerid_human(match_id, new_pairs[i], i)
                n_queries += 1
            end
        end
    
        if length(ai_types) == 1
            stmt_updt_playerid_ai(match_id, first(ai_types))
            n_queries += 1
        end

        stmt_updt_source_recorder_all_dead(match_id, all_dead)
        n_queries += 1
    
        for i in keys(team_victory)
            stmt_update_team_victory(match_id, i, team_victory[i])
            n_queries += 1
        end

        return nothing
    end
    
    function update_recorder_uberids(match)
        update_id_count += 1
    
        lobbyid::LobbyId = lobbyid_transform(match.lobbyid)
        
        timestamp::Timestamp = match.timestamp + (4 * 3600)
    
        match_id::MatchId = generate_match_id(timestamp, lobbyid)
        
        known_uberids = Dict{Username, UberId}()
        unpaired_usernames::Vector{Username} = Vector{Username}()
    
        for i in stmt_find_matched_player(match_id)
            if i.player_id == "-1"
                push!(unpaired_usernames, i.username)
            else
                push!(known_uberids, i.player_id)
            end
        end
    
        if !(isempty(unpaired_usernames))
            uberids = split_stringarr(match.uberids)
            unpaired_uberids::Vector{UberId} = setdiff(uberids, values(known_uberids))
    
            new_pairs = pair_uberids(unpaired_usernames, unpaired_uberids, timestamp, stmt_get_namehist, stmt_get_ubernames)
        end
    
        for i in unpaired_usernames
            stmt_updt_playerid_human(match_id, new_pairs[i], i)
            n_queries += 1
        end
    
        stmt_updt_source_recorder(match_id)
        n_queries += 1
    end
    
    function insert_recorder_match(match, armies, is_qbe::Bool)
        insert_count += 1
    
        lobbyid = lobbyid_transform(match.lobbyid)
        time_start::Timestamp = match.timestamp + (4 * 3600)
        match_id = generate_match_id(time_start, lobbyid)
        patch = match.version
        duration = match.duration
    
        titans = match.titans
    
        all_dead = armies[1]["defeated"] && armies[2]["defeated"]

        stmt_insert_match(
            match_id, lobbyid, time_start, duration,
            titans, all_dead, patch
        )
        n_queries += 1
    
        usernames::Vector{Username} = Vector{Username}()
        for army in armies
            if !army["ai"]
                usernames = vcat(usernames, army["players"]) 
            end
        end
    
        uberids::Vector{UberId} = split_stringarr(match.uberids)
    
        uberid_of::Dict{Username, UberId} = pair_uberids(usernames, uberids, time_start, stmt_get_namehist, stmt_get_ubernames)
    
        counter = 1
        counter2 = 1
    
        for army in armies
            size = max(length(army["players"]), 1)
            shared = size > 1
            win = !army["defeated"]
            team_num = counter

            stmt_insert_team(match_id, team_num, win, shared, size)
            n_queries += 1
    
            eco10 = round(Int16, 10 * army["econ"])
            player_type = (if army["ai"] "aiDiff" else "pa inc" end)
    
            if army["ai"]
                player_num = counter2
                username = "Lost AI Username ($player_num)"
                player_id = if length(army["players"]) > 0
                                fix_aidiff(army["players"][1])
                            else
                                "Unknown AI Recorder"
                            end
                if is_qbe
                    player_id = player_id * " QBE"
                end
                commanders = size

                stmt_insert_army(
                    match_id, player_num, team_num, username,
                    player_type, player_id, eco10, commanders
                )
                n_queries += 1
                counter2 += 1
            else
                for player in army["players"]
                    player_num = counter2
                    username = player
                    player_id = uberid_of[username]
                    commanders = size
    
                    stmt_insert_army(
                        match_id, player_num, team_num, username,
                        player_type, player_id, eco10, commanders
                    )
                    n_queries += 1
                    counter2 += 1
                end
            end
            
            counter += 1
        end
    end
    
    function insert_recorder_match_FFA(match, armies, is_qbe::Bool)
        insert_count += 1
    
        lobbyid = lobbyid_transform(match.lobbyid)
        time_start::Timestamp = match.timestamp + (4 * 3600)
        match_id = generate_match_id(time_start, lobbyid)
        patch = match.version
        duration = match.duration
    
        titans = match.titans
    
        all_dead::Bool = true
        for army in armies
            all_dead = army["defeated"]
            if !army["defeated"]
                break
            end
        end

        stmt_insert_match(
            match_id, lobbyid, time_start, duration,
            titans, all_dead, patch
        )
        n_queries += 1
    
        usernames::Vector{Username} = Vector{Username}()
        for army in armies
            if !army["ai"]
                usernames = vcat(usernames, army["players"]) 
            end
        end
    
        uberids::Vector{UberId} = split_stringarr(match.uberids)
    
        uberid_of::Dict{Username, UberId} = pair_uberids(usernames, uberids, time_start, stmt_get_namehist, stmt_get_ubernames)
    
        counter = 1
    
        for army in armies
            size = 1
            shared = false
            win = !army["defeated"]
            team_num = counter

            stmt_insert_team(match_id, team_num, win, shared, size)
            n_queries += 1
    
            eco10 = round(Int16, 10 * army["econ"])
            player_type = (if army["ai"] "aiDiff" else "pa inc" end)
    
            player_num = counter
            commanders = size
            if army["ai"]
                username = "Lost AI Username ($player_num)"
                player_id = if length(army["players"]) > 0
                                fix_aidiff(army["players"][1])
                            else
                                "Unknown AI Recorder"
                            end
                if is_qbe
                    player_id = player_id * " QBE"
                end
            else
                username = army["players"][1]
                player_id = uberid_of[username]
            end

            stmt_insert_army(
                match_id, player_num, team_num, username,
                player_type, player_id, eco10, commanders
            )
            n_queries += 1
            counter += 1
        end
    end

    lobbyids = [lobbyid_transform(i.lobbyid) for i in matches]
    ignore = get_ignore(lobbyids, conn)

    update = get_update(lobbyids, conn, Int32(timestamp_1 - (5 * 3600)), timestamp_2)
    update_lobbyids = Set{LobbyId}()
    for i in keys(update)
        push!(update_lobbyids, i[1])
    end

    sandbox = get_sandbox(lobbyids, conn)
    ffa = get_FFA(lobbyids, conn)
    qbe = get_QBE(lobbyids, conn)
    
    LibPQ.execute(conn, "BEGIN;")
    for match in matches
        lobbyid = lobbyid_transform(match.lobbyid)

        if lobbyid in sandbox
            continue
        end

        if lobbyid in ignore
            continue
        end

        is_qbe = lobbyid in qbe

        if !(ismissing(match.armies))
            armies = JSON.parse(match.armies)

            if lobbyid in update_lobbyids
                update_recorder_match(match, armies, is_qbe, update)
                stmt_push_gamefeed_to_matches(lobbyid, match.timestamp)
                n_queries += 1
            elseif length(armies) == 2
                insert_recorder_match(match, armies, is_qbe)
                stmt_push_gamefeed_to_matches(lobbyid, match.timestamp)
                n_queries += 1
            elseif lobbyid in ffa
                insert_recorder_match_FFA(match, armies, is_qbe)
                stmt_push_gamefeed_to_matches(lobbyid, match.timestamp)
                n_queries += 1
            end
        elseif lobbyid in update_lobbyids
            update_recorder_uberids(match)
            stmt_push_gamefeed_to_matches(lobbyid, match.timestamp)
            n_queries += 1
        end
    end

    print(
        "$update_full_count matches updated\n",
        "$update_id_count matches id-verified\n",
        "$insert_count matches inserted\n",
        "$n_queries total queries\n"
    )

    LibPQ.execute(conn, "COMMIT;")
end

function import_recorder(interval::Int64 = 12500)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")
    # merge_legacy_namehist(conn)

    max_time::Int64 = first(LibPQ.execute(conn, 
        "SELECT MAX(timestamp) FROM reckoner.replayfeed;"
    )).max

    last_time::Timestamp = open("last_recorder_timestamp") do infile
        last_time = parse(Int64, read(infile, String))
    end

    stmt_get_replayfeed = LibPQ.prepare(conn, "
        SELECT *
        FROM reckoner.replayfeed
        WHERE timestamp > \$1
        ORDER BY timestamp ASC
        LIMIT $interval;"
    )

    while last_time < max_time
        last_time -= 1800

        res = stmt_get_replayfeed(last_time) |> Tables.rowtable

        process_matches(res, conn, last_time, res[end].timestamp)

        last_time = res[end].timestamp

        open("last_recorder_timestamp", "w") do outfile
            write(outfile, string(last_time))
        end
    end

end
