import CSV
import Dates
import Parsers
import LibPQ
import Tables

include("reckoner_common.jl")

struct Match
    time_start::Int32
    time_end::Int32
    winner::Int32
end

function Match(in)
    format = Dates.DateFormat("yyyy-mm-dd HH:MM:SS.sss")
    time_start::Int32 = floor(Int32, Dates.datetime2unix(Dates.DateTime(in.Date, format))) - 3600 # River sent me times in CET
    time_end::Int32 = floor(Int32, Dates.datetime2unix(Dates.DateTime(in.EndTime, format))) - 3600

    winner = in.WinningTeamId

    Match(time_start, time_end, winner)
end

function import_river()
    uberids = Dict{UInt32, UInt64}()
    matches = Dict{Int64, Match}()
    teams = Dict{Int64, Dict{Int64, UInt64}}()
    replayfed = Set{Int64}()
    
    customs = Vector{Int64}()
    mangled = Vector{Int64}()

    for row in CSV.File("static_data_sources/river/players.csv"; typemap=Dict(:Id => UInt32, :UberId => UInt64))
        uberids[row.Id] = row.UberId
    end

    for row in CSV.File("static_data_sources/river/matches.csv"; typemap = Dict(:WinningTeamId => Int32, :Id => String))
        lobbyid = lobbyid_transform(row.Id)
        matches[lobbyid] = Match(row)

        if row.Source == "Uber"
            push!(replayfed, lobbyid)
        end

        if row.Id[1:7] == "custom_"
            push!(customs, lobbyid)
        else
            push!(mangled, lobbyid)
        end
    end
    delete!(replayfed, 744289582832834192) # OMG this is a 2v2v2 on Pax where 2 teams are shared, NOT A 1v1 :/

    for row in CSV.File("static_data_sources/river/teams.csv"; 
                    typemap = Dict(:TeamId => Int64, :MatchId => UInt64, :PlayerId => UInt32))
        lobbyid = lobbyid_transform(row.MatchId)

        if lobbyid in keys(teams)
            teams[lobbyid][row.TeamId] = uberids[row.PlayerId]
        else
            teams[lobbyid] = Dict(row.TeamId => uberids[row.PlayerId])
        end
    end

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    query::String = "   SELECT lobbyid, time_start
                        FROM reckoner.matches a 
                        INNER JOIN reckoner.match_aggregate b
                        ON a.match_id = b.match_id
                        WHERE time_start < 1562284800
                        AND server = 'pa inc'
                        AND source_superstats = TRUE;
                        "

    existing = Dict{Int64, Any}()

    for row in Tables.rowtable(LibPQ.execute(conn, query))
        existing[row.lobbyid] = row
    end

    river_li = sort(mangled)
    existing_li = sort(collect(keys(existing)))

    true_li = Dict{Int64, Int64}()
    true_uberid = Dict{UInt64, UInt64}()

    i::Int64 = 1
    j::Int64 = 1

    round_err::Int64 = 1000000

    while (i <= length(river_li) && j <= length(existing_li))
        if (river_li[i] > (existing_li[j] + round_err))
            j += 1
        elseif ((river_li[i] + round_err) < (existing_li[j]))
            i += 1
        else
            if (abs(matches[river_li[i]].time_start - existing[existing_li[j]].time_start) < 3600) # timestamps within an hour
                true_li[river_li[i]] = existing_li[j]
                i += 1
                
                rewind_distance::Int64 = 0 # j -= 1 until we get at least 100,000 backwards
                while (rewind_distance < round_err)
                    if (j != 1)
                        rewind_distance += (existing_li[j] - existing_li[j+1])
                        j -= 1
                    else
                        rewind_distance = round_err
                    end
                end
            else
                j += 1
            end
        end
    end

    function pair_true_uberids(mangled_uberids::Vector{UInt64}, true_uberids::Vector{UInt64})
        mangled = sort(mangled_uberids)
        truth = sort(true_uberids)

        k::Int64 = 1
        l::Int64 = 1

        while (k < length(mangled) && l < length(truth))
            if (mangled[k] > (truth[l] + round_err))
                l += 1
            elseif ((mangled[k] + round_err) < truth[l])
                k += 1
            else
                if !(mangled[k] in keys(true_uberid))
                    true_uberid[mangled[k]] = truth[l]
                end

                l += 1                
                k = 1 
            end
        end
    end

    # custom lobbyids are luckily unmangled, so we can just query directly for them

    query = "   SELECT lobbyid, player_id
                FROM reckoner.armies a 
                INNER JOIN reckoner.matches b
                ON a.match_id = b.match_id
                WHERE player_type = 'pa inc'
                AND lobbyid IN ("
    
    for i in customs
        query = query * "$i, "
    end

    query = query[1:end - 2] * ");"

    true_customs = Dict{Int64, Vector{UInt64}}()

    for row in Tables.rowtable(LibPQ.execute(conn, query))
        uberid = Parsers.parse(UInt64, row.player_id)
        if row.lobbyid in keys(true_customs)
            push!(true_customs[row.lobbyid], uberid)
        else
            true_customs[row.lobbyid] = [uberid]
        end
    end

    for i in customs
        if i in keys(true_customs)
            pair_true_uberids(collect(values(teams[i])), true_customs[i])
        end
    end

    # matched non-custom games have mangled uberids

    print(length(true_li), "\n")

    if length(true_li) > 0
        query = "   SELECT lobbyid, player_id
                    FROM reckoner.armies a
                    INNER JOIN reckoner.matches b
                    ON a.match_id = b.match_id
                    WHERE player_type = 'pa inc'
                    AND lobbyid IN ("
        
        for i in keys(true_li)
            query = query * "$(true_li[i]), "
        end

        query = query[1:end - 2] * ");"

        true_uberid_obs = Dict{Int64, Vector{UInt64}}()

        for row in Tables.rowtable(LibPQ.execute(conn, query))
            uberid = Parsers.parse(UInt64, row.player_id)
            if row.lobbyid in keys(true_uberid_obs)
                push!(true_uberid_obs[row.lobbyid], uberid)
            else
                true_uberid_obs[row.lobbyid] = [uberid]
            end
        end

        for i in keys(true_li)
            pair_true_uberids(collect(values(teams[i])), true_uberid_obs[true_li[i]])
        end
    end

    guaranted_uberid_matches = length(true_uberid)

    # let's match less sure uberids now
    query = "   SELECT DISTINCT(player_id)
                FROM reckoner.armies
                WHERE player_type = 'pa inc'
                AND player_id NOT IN ("
    
    for i in values(true_uberid)
        query = query * "'$i', "
    end

    query = query[1:end-2] * ");"

    observed_uberids = Vector{UInt64}()

    for row in Tables.rowtable(LibPQ.execute(conn, query))
        push!(observed_uberids, Parsers.parse(UInt64, row.player_id))
    end
        
    pair_true_uberids(collect(values(uberids)), observed_uberids)

    replayfeed_update = Vector{Int64}()

    matched_matches = keys(true_li)

    print("checkpoint 1\n")

    for i in replayfed
        if i in matched_matches
            push!(replayfeed_update, i)
        end
    end

    for i in replayfeed_update
        teams[true_li[i]] = teams[i]
    end

    print("checkpoint 2\n")

    query = "   SELECT lobbyid, player_id, team_num, a.match_id
                FROM reckoner.armies a
                INNER JOIN reckoner.matches b
                ON a.match_id = b.match_id
                WHERE lobbyid IN ("
        
    for i in replayfeed_update
        query = query * "$(true_li[i]), "
    end

    query = query[1:end - 2] * ");"

    print("checkpoint 3\n")

    team_update_id = Dict{Tuple{Int64, Int32}, Int64}() # (Mangled LobbyID, River Team Number) => Reckoner Team Number

    previously_unknown = Set{Tuple{Int64, Int64}}() # (Mangled LobbyID, River Team Number) if player_id was -1 before

    curr_team_ids = Dict{Tuple{Int64, Int16}, Int128}() # (True LobbyID, Reckoner Team Number) => Reckoner Player Id

    match_ids = Dict{Int64, Int64}() # takes a Reckoner lobbyid, returns a Reckoner match_id

    for row in Tables.rowtable(LibPQ.execute(conn, query))
        curr_team_ids[(row.lobbyid, row.team_num)] = Parsers.parse(Int128, row.player_id)

        match_ids[row.lobbyid] = row.match_id
    end

    print("checkpoint 4\n")
    
    team_keys = keys(teams)
    true_uberid_keys = keys(true_uberid)

    for i in replayfeed_update
        if !(i in team_keys)
            continue # ignore entry, jump to next
        end

        river_teams = teams[i]
        rt_keys = collect(keys(river_teams))

        a = rt_keys[1]
        b = rt_keys[2]

        curr_tli = true_li[i]

        if river_teams[a] in true_uberid_keys
            if true_uberid[river_teams[a]] == curr_team_ids[(curr_tli, 1)]
                team_update_id[(i, a)] = 1
                team_update_id[(i, b)] = 2
                if curr_team_ids[(curr_tli, 2)] == -1
                    push!(previously_unknown, (i, b))
                end
                continue
            elseif true_uberid[river_teams[a]] == curr_team_ids[(curr_tli, 2)]
                team_update_id[(i, a)] = 2
                team_update_id[(i, b)] = 1
                if curr_team_ids[(curr_tli, 1)] == -1
                    push!(previously_unknown, (i, b))
                end
                continue
            end
        end

        if river_teams[b] in true_uberid_keys
            if true_uberid[river_teams[b]] == curr_team_ids[(curr_tli, 2)]
                team_update_id[(i, a)] = 1
                team_update_id[(i, b)] = 2
                if curr_team_ids[(curr_tli, 1)] == -1
                    push!(previously_unknown, (i, a))
                end
                continue
            elseif true_uberid[river_teams[b]] == curr_team_ids[(curr_tli, 1)]
                team_update_id[(i, a)] = 2
                team_update_id[(i, b)] = 1
                if curr_team_ids[(curr_tli, 2)] == -1
                    push!(previously_unknown, (i, a))
                end
                continue
            end
        end

        print("WARNING!\n")
        print(curr_tli)
    end

    print("checkpoint 5\n")

    print(  "total matches: $(length(matches))\n",
            "replayfeed matches: $(length(replayfed))\n",
            "mangled matches: $(length(river_li))\n",
            "matched matches: $(length(true_li))\n",
            "new matches: $(length(river_li) - length(true_li))\n",
            "total players: $(length(uberids))\n",
            "100% matched players: $guaranted_uberid_matches\n",
            "matched players: $(length(true_uberid))\n",
            "reported ties: $(sum([i[2].winner == -1 for i in matches]))\n",
            "updating matches: $(length(replayfeed_update))\n"
    )

    LibPQ.execute(conn, "BEGIN;")

    for i in replayfed
        if !(i in team_keys)
            continue # ignore entry, jump to next
        end

        if i in replayfeed_update
            true_lobbyid = true_li[i]
            query = "   UPDATE reckoner.matches
                        SET source_river = TRUE,
                        all_dead = $(matches[i].winner == -1)
                        WHERE lobbyid = $true_lobbyid;
                        "
            LibPQ.execute(conn, query)

            match_id = match_ids[true_lobbyid]
            for j in keys(teams[i])
                true_team_num = team_update_id[(i, j)]
                query = "   UPDATE reckoner.teams a
                            SET win = $(j == matches[i].winner)
                            WHERE match_id = $match_id
                            AND team_num = $true_team_num;
                            "
                LibPQ.execute(conn, query)
                if (i, j) in previously_unknown
                    query = "   UPDATE reckoner.armies
                                SET player_type = 'river',
                                player_id = '$(teams[i][j])'
                                WHERE match_id = $match_id
                                AND team_num = $true_team_num;
                                "
                    LibPQ.execute(conn, query)
                end
            end
        else
            match_id = match_id_generation(matches[i].time_start, ["irrelevant"], "river", i)
            query = "   INSERT INTO reckoner.matches
                        (source_river, server, lobbyid, 
                        all_dead, time_start, time_end,
                        match_id)
                        VALUES 
                        (TRUE, 'river', '$i',
                        $(matches[i].winner == -1), 
                        $(matches[i].time_start),
                        $(matches[i].time_end),
                        $match_id);
                        "
            LibPQ.execute(conn, query)

            for j in keys(teams[i])
                if (j == minimum(keys(teams[i])))
                    team_num = 1
                else
                    team_num = 2
                end
                query = "   INSERT INTO reckoner.teams
                            (match_id, team_num, win, shared, size)
                            VALUES
                            ($match_id, $team_num, 
                            $(j == matches[i].winner), false, 1);
                            "
                LibPQ.execute(conn, query)

                query = "   INSERT INTO reckoner.armies
                            (match_id, team_num, player_num,
                            player_type, player_id, commanders)
                            VALUES
                            ($match_id, $team_num, $team_num,
                            'river', $(teams[i][j]), 1);
                            "
                LibPQ.execute(conn, query)
            end
        end
    end

    for (i, j) in true_uberid
        query = "   INSERT INTO reckoner.smurfs
                    (alt_player_type, alt_player_id,
                    main_player_type, main_player_id)
                    VALUES ('river', '$i', 'pa inc', '$j')
                    ON CONFLICT DO NOTHING;
                    "
        LibPQ.execute(conn, query)
    end

    LibPQ.execute(conn, "COMMIT;")

    fix = "UPDATE reckoner.smurfs a
        SET main_player_id = b.main_player_id,
        main_player_type = b.main_player_type
        FROM reckoner.smurfs b
        WHERE (a.main_player_type, a.main_player_id) 
        = (b.alt_player_type, b.alt_player_id);"    

    LibPQ.execute(conn, fix)
end