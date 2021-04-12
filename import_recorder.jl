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
    while index < (n + interval)
        query::String = "   SELECT * FROM reckoner.namehist
                            ORDER BY (uberid, name)
                            LIMIT $(interval)
                            OFFSET $(index);"

        for i in LibPQ.execute(conn, query)
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

function update_recorder_match(match, armies, is_qbe::Bool, update::Dict{Tuple{LobbyId, Username}, PastComposite}, conn)::Vector{String}
    output = Vector{String}()

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
                    return output
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
        query::String = "   SELECT DISTINCT(team_num)
                            FROM reckoner.armies
                            WHERE player_id = '$ai_victor'
                            OR player_id = 'AI-Unknown';
                            "
        
        res = LibPQ.execute(conn, query) |> Tables.rowtable

        if length(res) == 1
            team_victory[res[1].team_num] = true
        end
    end

    if "-1" in values(known_uberids)
        uberids = split_stringarr(match.uberids)
        unpaired_uberids::Vector{UberId} = setdiff(uberids, values(known_uberids))
        unpaired_usernames::Vector{Username} = usernames[[known_uberids[i] != "-1" for i in usernames]]

        new_pairs = pair_uberids(unpaired_usernames, unpaired_uberids, timestamp, conn)

        for i in unpaired_usernames
            query = "   UPDATE reckoner.armies
                        SET player_type = 'pa inc',
                        player_id = '$(new_pairs[i])'
                        WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500)
                        AND username = '$(sanitize(i))';"
            push!(output, query)
        end
    end

    if length(ai_types) == 1
        query = "   UPDATE reckoner.armies
                    SET player_id = '$(first(ai_types))'
                    WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500)
                    AND player_type = 'aiDiff'
                    AND player_id = 'AI-Unknown';"
        push!(output, query)
    end

    query = "   UPDATE reckoner.matches
                SET source_recorder = TRUE,
                all_dead = $all_dead
                WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500);"
    push!(output, query)

    for i in keys(team_victory)
        query = "   UPDATE reckoner.teams
                    SET win = $(team_victory[i])
                    WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500)
                    AND team_num = $i;"
        push!(output, query)
    end

    output
end

function update_recorder_uberids(match, update::Dict{Tuple{LobbyId, Username}, PastComposite}, conn)::Vector{String}
    output = Vector{String}()

    lobbyid::LobbyId = lobbyid_transform(match.lobbyid)
    
    timestamp::Timestamp = match.timestamp + (4 * 3600)

    match_id::MatchId = generate_match_id(timestamp, lobbyid)
    
    known_uberids = Dict{Username, UberId}()
    unpaired_usernames::Vector{Username} = Vector{Username}()
    
    query::String = "   SELECT username, player_id
                        FROM reckoner.armies
                        WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500)
                        AND player_type IN ('superstats', 'pa inc');
                        "

    for i in LibPQ.execute(conn, query)
        if i.player_id == "-1"
            push!(unpaired_usernames, i.username)
        else
            push!(known_uberids, i.player_id)
        end
    end

    if !(isempty(unpaired_usernames))
        uberids = split_stringarr(match.uberids)
        unpaired_uberids::Vector{UberId} = setdiff(uberids, values(known_uberids))

        new_pairs = pair_uberids(unpaired_usernames, unpaired_uberids, timestamp, conn)
    end

    for i in unpaired_usernames
        query = "   UPDATE reckoner.armies
                    SET player_type = 'pa inc',
                    player_id = '$(new_pairs[i])'
                    WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500)
                    AND username = '$(sanitize(i))';"
        push!(output, query)
    end

    query = "   UPDATE reckoner.matches
                SET source_recorder = TRUE
                WHERE match_id BETWEEN ($match_id - 500) AND ($match_id + 500);"
    push!(output, query)

    output
end

function insert_recorder_match(match, armies, is_qbe::Bool, conn)::Vector{String}
    output = Vector{String}()

    lobbyid = lobbyid_transform(match.lobbyid)
    time_start::Timestamp = match.timestamp + (4 * 3600)
    match_id = generate_match_id(time_start, lobbyid)
    patch = match.version
    duration = match.duration

    titans = match.titans

    all_dead = armies[1]["defeated"] && armies[2]["defeated"]

    query::String = "   INSERT INTO reckoner.matches 
                        (match_id, lobbyid, time_start,
                        duration, titans, all_dead, 
                        patch, source_recorder, server)
                        VALUES
                        ($match_id, $lobbyid, $time_start,
                        $duration, $titans, $all_dead,
                        $patch, TRUE, 'pa inc');
                        "
    push!(output, query)

    usernames::Vector{Username} = Vector{Username}()
    for army in armies
        if !army["ai"]
            usernames = vcat(usernames, army["players"]) 
        end
    end

    uberids::Vector{UberId} = split_stringarr(match.uberids)

    uberid_of::Dict{Username, UberId} = pair_uberids(usernames, uberids, time_start, conn)

    counter = 1
    counter2 = 1

    for army in armies
        size = max(length(army["players"]), 1)
        shared = size > 1
        win = !army["defeated"]
        team_num = counter

        query = "   INSERT INTO reckoner.teams
                    (match_id, team_num, win,
                    shared, size)
                    VALUES
                    ($match_id, $team_num, $win,
                    $shared, $size);
                    "
        push!(output, query)

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

            query = "   INSERT INTO reckoner.armies
                        (match_id, player_num, team_num,
                        username, player_type, player_id,
                        eco10, commanders)
                        VALUES
                        ($match_id, $player_num, $team_num,
                        '$username', '$player_type', '$player_id',
                        $eco10, $commanders)
                        "
            push!(output, query)
            counter2 += 1
        else
            for player in army["players"]
                player_num = counter2
                username = player
                player_id = uberid_of[username]
                commanders = size

                query = "   INSERT INTO reckoner.armies
                            (match_id, player_num, team_num,
                            username, player_type, player_id,
                            eco10, commanders)
                            VALUES
                            ($match_id, $player_num, $team_num,
                            '$(sanitize(username))', '$player_type', '$player_id',
                            $eco10, $commanders)
                            "
                push!(output, query)
                counter2 += 1
            end
        end
        
        counter += 1
    end

    output
end

function insert_recorder_match_FFA(match, armies, is_qbe::Bool, conn)::Vector{String}
    output = Vector{String}()

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

    query::String = "   INSERT INTO reckoner.matches 
                        (match_id, lobbyid, time_start,
                        duration, titans, all_dead, 
                        patch, source_recorder, server)
                        VALUES
                        ($match_id, $lobbyid, $time_start,
                        $duration, $titans, $all_dead,
                        $patch, TRUE, 'pa inc');
                        "
    push!(output, query)

    usernames::Vector{Username} = Vector{Username}()
    for army in armies
        if !army["ai"]
            usernames = vcat(usernames, army["players"]) 
        end
    end

    uberids::Vector{UberId} = split_stringarr(match.uberids)

    uberid_of::Dict{Username, UberId} = pair_uberids(usernames, uberids, time_start, conn)

    counter = 1

    for army in armies
        size = 1
        shared = false
        win = !army["defeated"]
        team_num = counter

        query = "   INSERT INTO reckoner.teams
                    (match_id, team_num, win,
                    shared, size)
                    VALUES
                    ($match_id, $team_num, $win,
                    $shared, $size);
                    "
        push!(output, query)

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
        query = "   INSERT INTO reckoner.armies
                    (match_id, player_num, team_num,
                    username, player_type, player_id,
                    eco10, commanders)
                    VALUES
                    ($match_id, $player_num, $team_num,
                    '$(sanitize(username))', '$player_type', '$player_id',
                    $eco10, $commanders)
                    "
        push!(output, query)
        
        counter += 1
    end

    output
end

function process_matches(matches, conn, timestamp_1::Timestamp, timestamp_2::Timestamp)
    update_full_count = 0
    update_id_count = 0
    insert_count = 0

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

    queries = Vector{String}()
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
                queries = vcat(queries, update_recorder_match(match, armies, is_qbe, update, conn))
                update_full_count += 1
                push!(queries, push_gamefeed_to_matches(lobbyid, match.timestamp))
            elseif length(armies) == 2
                queries = vcat(queries, insert_recorder_match(match, armies, is_qbe, conn))
                insert_count += 1
                push!(queries, push_gamefeed_to_matches(lobbyid, match.timestamp))
            elseif lobbyid in ffa
                queries = vcat(queries, insert_recorder_match_FFA(match, armies, is_qbe, conn))
                insert_count += 1
                push!(queries, push_gamefeed_to_matches(lobbyid, match.timestamp))
            end
        elseif lobbyid in update_lobbyids
            queries = vcat(queries, update_recorder_uberids(match, update, conn))
            update_id_count += 1
            push!(queries, push_gamefeed_to_matches(lobbyid, match.timestamp))
        end
    end

    print("$update_full_count matches updated\n",
            "$update_id_count matches id-verified\n",
            "$insert_count matches inserted\n",
            "$(length(queries)) total queries\n")

    LibPQ.execute(conn, "BEGIN;")

    for query in queries
        LibPQ.execute(conn, query)
    end

    LibPQ.execute(conn, "COMMIT;")
end

function import_recorder()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")
    # merge_legacy_namehist(conn)

    max_time::Int64 = first(LibPQ.execute(conn, "SELECT MAX(timestamp) FROM reckoner.replayfeed;")).max

    last_time::Timestamp = open("last_recorder_timestamp") do infile
        last_time = parse(Int64, read(infile, String))
    end

    interval::Int64 = 12500
    while last_time < max_time
        last_time -= 1800

        query::String = "   SELECT *
                            FROM reckoner.replayfeed
                            WHERE timestamp > $last_time
                            ORDER BY timestamp ASC
                            LIMIT $(interval)
                            ;"

        res = LibPQ.execute(conn, query) |> Tables.rowtable

        process_matches(res, conn, last_time, res[end].timestamp)

        last_time = res[end].timestamp

        open("last_recorder_timestamp", "w") do outfile
            write(outfile, string(last_time))
        end
    end

end
