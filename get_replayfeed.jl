import HTTP
import JSON
import LibPQ
import Tables
import Dates

include("secrets.jl")
include("name_history.jl")
include("replayfeed_common.jl")

const FeedMatch = Dict{String, Any}
const ReplayFeed = Vector{Any}

function rem_pte(patch::Integer)::Int64
    patch
end

function rem_pte(patch::String)::Int64
    out = tryparse(Int64, patch)

    if out isa Nothing
        tryparse(Int64, replace(patch, "-pte" => ""))
    end

    out
end

function process_replayfeed(replayfeed::ReplayFeed, conn; match_time_range::Int64 = 1000)
    update_full_count = 0
    update_id_count = 0
    insert_count = 0
    n_queries = 0

    stmt_find_match_players = LibPQ.prepare(conn, "
        SELECT username, player_id
        FROM reckoner.armies
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range)
        AND player_type IN ('superstats', 'pa inc');"
    )

    stmt_updt_playerid = LibPQ.prepare(conn, "   
        UPDATE reckoner.armies
        SET player_type = 'pa inc',
        player_id = \$2
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range)
        AND username = \$3;"
    )

    stmt_updt_match_full = LibPQ.prepare(conn, "   
        UPDATE reckoner.matches
        SET source_recorder = TRUE,
        all_dead = \$2
        WHERE match_id BETWEEN (\$1::BIGINT - $match_time_range) AND (\$1 + $match_time_range);"
    )

    stmt_updt_match_source = LibPQ.prepare(conn, "   
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
            patch, source_replayfeed, server
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

    function update_match(match::FeedMatch, player_metadata::Dict{Username, PastComposite})
        lobbyid = lobbyid_transform(match["LobbyId"])
        
        timestamp::Timestamp = floor(Timestamp, Dates.datetime2unix(Dates.DateTime(match["MatchBeginTimeString"], DATEFORMAT))) 
    
        match_id = generate_match_id(timestamp, lobbyid)
        
        usernames = Vector{Username}()
        known_uberids = Dict{Username, UberId}()
    
        all_dead = true
        team_victory = Dict{TeamNum, Bool}()
        
        for i in match["ReplayInfoJson"]["armies"]
            usernames = vcat(usernames, i["players"])
    
            for j in i["players"]
                push!(known_uberids, j => player_metadata[j].uberid)
            end
            
            curr_username = if (i["ai"]) i["name"] else i["players"][1] end

            if curr_username in keys(player_metadata)
                team_num = player_metadata[curr_username].team_num
            else
                name_components = split(curr_username, " ")
                candidate_names = [
                    name_components[1], 
                    name_components[1] * name_components[2],
                    name_components[1] * name_components[2] * name_components[3]
                ]
                for candidate in candidate_names
                    if candidate in keys(player_metadata)
                        team_num = player_metadata[candidate].team_num
                        break
                    end
                end
            end

            alive = !(i["defeated"])
            if !(team_num in keys(team_victory))
                team_victory[team_num] = alive
            elseif !(team_victory[team_num])
                team_victory[team_num] = alive
            end
    
            if alive
                all_dead = false
            end
        end
    
        if "-1" in values(known_uberids)
            unpaired_uberids::Vector{UberId} = setdiff(match["ParticipatingUberIds"], values(known_uberids))
            unpaired_usernames::Vector{Username} = usernames[[known_uberids[i] != "-1" for i in usernames]]
    
            new_pairs = pair_uberids(unpaired_usernames, unpaired_uberids, timestamp, stmt_get_namehist, stmt_get_ubernames)
            
            for i in unpaired_usernames
                stmt_updt_playerid(match_id, new_pairs[i], i)
                n_queries += 1
            end
        end
    
        stmt_updt_match_full(match_id, all_dead)
        n_queries += 1
    
        for i in keys(team_victory)
            stmt_update_team_victory(match_id, i, team_victory[i])
            n_queries += 1
        end
        
        stmt_push_gamefeed_to_matches(lobbyid, timestamp)
        n_queries += 1

        update_full_count += 1
    end
    
    function update_uberids(match::FeedMatch)
        lobbyid = lobbyid_transform(match["LobbyId"])
        
        timestamp::Timestamp = floor(Timestamp, Dates.datetime2unix(Dates.DateTime(match["MatchBeginTimeString"], DATEFORMAT))) 
    
        match_id = generate_match_id(timestamp, lobbyid)
        
        known_uberids = Set{UberId}()
        unpaired_usernames::Vector{Username} = Vector{Username}()
    
        for i in stmt_find_match_players(match_id)
            if i.player_id == "-1"
                push!(unpaired_usernames, i.username)
            else
                push!(known_uberids, i.player_id)
            end
        end
    
        if !(isempty(unpaired_usernames))
            unpaired_uberids::Vector{UberId} = setdiff(match["ParticipatingUberIds"], values(known_uberids))
    
            new_pairs = pair_uberids(unpaired_usernames, unpaired_uberids, timestamp, stmt_get_namehist, stmt_get_ubernames)
        end
    
    
        for i in unpaired_usernames
            stmt_updt_playerid(match_id, new_pairs[i], i)
            n_queries += 1
        end
    
        stmt_updt_match_source(match_id)
        n_queries += 1
        
        stmt_push_gamefeed_to_matches(lobbyid, timestamp)
        n_queries += 1

        update_id_count += 1
    end
    
    function insert_match(match::FeedMatch)
        lobbyid = lobbyid_transform(match["LobbyId"])
        time_start::Timestamp = floor(Timestamp, Dates.datetime2unix(Dates.DateTime(match["MatchBeginTimeString"], DATEFORMAT))) 
        match_id = generate_match_id(time_start, lobbyid)
        patch = rem_pte(match["BuildVersion"])
        duration = match["Duration"]
    
        titans = "PAExpansion1" in match["ReplayInfoJson"]["required_content"]
    
        armies = match["ReplayInfoJson"]["armies"]
    
        all_dead = armies[1]["defeated"] && armies[2]["defeated"]
    
        stmt_insert_match(
            match_id, lobbyid, time_start, duration,
            titans, all_dead, patch
        )
        n_queries += 1
    
        usernames::Vector{Username} = Vector{Username}()
        for army in armies
            usernames = vcat(usernames, army["players"]) 
        end
    
        uberids::Vector{UberId} = match["ParticipatingUberIds"]
    
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
                username = sanitize(army["name"])
                
                player_id = if "name" in keys(army["personality"])
                                army["personality"]["name"]
                            elseif "personality_tag" in keys(army["personality"])
                                army["personality"]["personality_tag"]
                            else
                                "Unknown AI Replayfeed"
                            end
                if match["qbe"] || ("qbe" in keys(army["personality"]) && army["personality"]["qbe"])
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
     
        stmt_push_gamefeed_to_matches(lobbyid, time_start)
        n_queries += 1

        insert_count += 1
    end
    
    function insert_match_FFA(match::FeedMatch)
        lobbyid = lobbyid_transform(match["LobbyId"])
        time_start::Timestamp = floor(Timestamp, Dates.datetime2unix(Dates.DateTime(match["MatchBeginTimeString"], DATEFORMAT))) 
        match_id = generate_match_id(time_start, lobbyid)
        patch = match["BuildVersion"]
        duration = match["Duration"]
    
        titans = "PAExpansion1" in match["ReplayInfoJson"]["required_content"]
    
        armies = match["ReplayInfoJson"]["armies"]
    
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
            usernames = vcat(usernames, army["players"]) 
        end
    
        uberids::Vector{UberId} = match["ParticipatingUberIds"]
    
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
                username = sanitize(army["name"])
                player_id = if "name" in keys(army["personality"])
                                army["personality"]["name"]
                            elseif "personality_tag" in keys(army["personality"])
                                army["personality"]["personality_tag"]
                            else
                                "Unknown AI Replayfeed"
                            end
                if match["qbe"] || ("qbe" in keys(army["personality"]) && army["personality"]["qbe"])
                    player_id = player_id * " QBE"
                end
            else
                if isempty(army["players"])
                    # Extremely rare edge case where the player fails to enter the game completely
                    continue 
                else
                    username = army["players"][1]
                    player_id = uberid_of[username]
                end
            end

            stmt_insert_army(
                match_id, player_num, team_num, username,
                player_type, player_id, eco10, commanders
            )
            n_queries += 1
            
            counter += 1
        end
    
        stmt_push_gamefeed_to_matches(lobbyid, time_start)
        n_queries += 1

        insert_count += 1
    end

    lobbyids = [lobbyid_transform(i["LobbyId"]) for i in replayfeed]
    ignore = get_ignore(lobbyids, conn)

    update = get_update(lobbyids, conn)
    update_lobbyids = Set{LobbyId}()
    for i in keys(update)
        push!(update_lobbyids, i[1])
    end

    sandbox = get_sandbox(lobbyids, conn)
    ffa = get_FFA(lobbyids, conn)

    qbe = get_QBE(lobbyids, conn)

    LibPQ.execute(conn, "BEGIN;")
    for match in replayfeed
        lobbyid = lobbyid_transform(match["LobbyId"])

        if lobbyid in sandbox
            continue
        end

        if lobbyid in ignore
            continue
        end

        match["ReplayInfoJson"] = JSON.parse(match["ReplayInfoJson"])

        match["qbe"] = lobbyid in qbe

        if !("error" in keys(match["ReplayInfoJson"]))
            if lobbyid in update_lobbyids
                update_match(match, update[lobbyid])
            elseif length(match["ReplayInfoJson"]["armies"]) == 2
                insert_match(match)
            elseif lobbyid in ffa
                insert_match_FFA(match)
            end
        elseif lobbyid in update_lobbyids
            update_uberids(match)
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

function get_replayfeed(conn)
    stmt_insert_uberid = LibPQ.prepare(conn, "
        INSERT INTO reckoner.ubernames (ubername, uberid)
        VALUES (\$1, \$2)
        ON CONFLICT DO NOTHING;"
    )
    url = "https://service.planetaryannihilation.net/GameClient/GetReplayList?MaxResults=9999"

    auth_url = "https://service.planetaryannihilation.net/GC/Authenticate"

    auth_payload = "{\"TitleId\": 4,\"AuthMethod\": \"UberCredentials\",\"UberName\": \"manlebtnureinmal\",\"Password\": \"$password\"}"

    auth_res = HTTP.request("POST", auth_url, [], auth_payload, cookies = true)

    auth_cookie = Dict{String, String}(
        "auth" => JSON.parse(String(auth_res.body))["SessionTicket"])

    res = HTTP.request("GET", url, cookies = auth_cookie)

    replayfeed = JSON.parse(String(res.body))["Games"]

    observed_uberids = Vector{String}()

    for i in replayfeed
        for j in i["ParticipatingUberIds"]
            push!(observed_uberids, j)
        end
    end

    name_obs::Vector{Observance} = Vector{Observance}()

    LibPQ.execute(conn, "BEGIN;")
    for i in observed_uberids
        names_url::String = "https://service.planetaryannihilation.net/GameClient/UserNames?TitleId=4&UberIds=$i"
        res = HTTP.request("GET", names_url, cookies = auth_cookie)
        names = JSON.parse(String(res.body))["Users"][i]

        push!(name_obs, Observance("pa inc", i, names["TitleDisplayName"], Int32(trunc(time()))))

        stmt_insert_uberid(names["UberName"], i)
    end
    LibPQ.execute(conn, "COMMIT;")

    name_hist_to_postgres(create_name_hist(name_obs))

    process_replayfeed(replayfeed, conn)
end

function get_replayfeed()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    try
        get_replayfeed(conn)
    finally
        LibPQ.close(conn)
    end
end