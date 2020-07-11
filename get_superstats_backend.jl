import LibPQ
import Tables

using Statistics

include("./reckoner_common.jl")
include("./name_history.jl")

struct Army
    match_id::Int64
    player_num::Int16
    username::String
    player_type::String
    uberid::String
    eco::Int16
    team_num::Int16
end

struct Team
    match_id::Int64
    team_num::Int16
    win::Bool
    shared::Bool
    size::Int32
    armies::Vector{Army}
end

struct Match
    match_id::Int64
    lobbyid::Int64
    duration::String
    time_start::Int32
    time_end::Union{Int32, String}
    titans::Bool
    ranked::Bool
    tourney::Bool
    mods::String
    mod_versions::String
    sources::String
    system_name::String
    system_info::String
    server::String
    teams::Vector{Team}
end

function Base.isless(left::Match, right::Match)
    left.match_id < right.match_id
end

function reformat_superstats_data(data::Array{Any,1})::Vector{Match}

    matches::Vector{Match} = Vector{Match}()

    for match in data
        playernames::Vector{String} = [sanitize(i["displayName"]) for i in collect(Iterators.flatten([j["extendedPlayers"] for j in match["armies"]]))]
        time_start::Int32 = Int32(match["gameStartTime"] ÷ 1000)
        if match["isCustomServer"]
            server::String = match["lobbyId"][end-5:end]
        else
            server = "pa inc"
        end
        match_id::Int64 = match_id_generation(time_start, playernames, server, match["lobbyId"])
        lobbyid::Int64 = lobbyid_transformation(match["lobbyId"])
        if match["gameEndTime"] != false
            time_end::Union{Int32, String} = Int32(match["gameEndTime"] ÷ 1000)
            duration::String = string(match["gameEndTime"] - match["gameStartTime"])
        else
            time_end = "NULL"
            duration = "NULL"
        end
        titans::Bool = match["isTitans"]
        ranked::Bool = match["isRanked"]
        tourney::Bool = match["tournamentInfo"]["isTournament"]
        mod_ids::Vector{String} = [i["identifier"] for i in match["serverMods"]]
        mods::String = format_array_postgres(sanitize.(mod_ids))
        mod_versions::String = format_array_postgres([sanitize(i["version"]) for i in match["serverMods"]])
        sources::String = format_array_postgres(["Super Stats",])
        system_name::String = sanitize(match["systemInfo"]["name"])
        system_info::String = sanitize(JSON.json(match["systemInfo"]["planets"]))
        teams::Vector{Team} = Vector{Team}()
        

        teamIds::Vector{Int32} = Vector{Int32}()
        shared_list::Vector{Bool} = Vector{Bool}()
        teamsize_list::Vector{Int16} = Vector{Int16}()
        playercount::Int16 = 0
        armies::Dict{Int16, Vector{Army}} = Dict{Int16, Vector{Army}}()
        for army in match["armies"]
            if army["ai"] == true
                if (typeof(army["aiDiff"]) == String)
                    uberid_list::Vector{String} = [sanitize(army["aiDiff"]),]
                    if (("com.pa.quitch.AIBugfixEnhancement" in mod_ids) |
                        ("com.pa.quitch.AIBugfixEnhancement-dev" in mod_ids))
                        uberid_list[1] *= " QBE"
                    end
                else
                    uberid_list = ["AI-Unknown",]
                end

                if uberid_list == ["",]
                    uberid_list = ["AI-Unknown",]
                end
            else
                uberid_list = [sanitize(player["uberId"]) for player in army["extendedPlayers"]]
            end
            commanders::Int16 = size(army["extendedPlayers"])[1]
            eco::Int16 = eco_transformation(army["econ_rate"])
            username_list::Vector{String} = [sanitize(player["displayName"]) for player in army["extendedPlayers"]]
            team_num::Union{Int16, Nothing} = findfirst(x -> x == army["teamId"], teamIds)
            if team_num isa Nothing # team_id not already assigned a team_num
                push!(teamIds, army["teamId"]) # initialize new teamId entry
                team_num = size(teamIds)[1] # team_num is new highest team_num
                push!(shared_list, commanders > 1) # initialize shared for this team
                push!(teamsize_list, commanders) # initialize teamsize for this team
                armies[team_num] = Vector{Army}() # initialize new armies vector for team
            else # team_id already assigned a new team_num
                teamsize_list[team_num] += commanders # add this many commanders to team size
            end
            for i in 1:(size(uberid_list)[1])
                playercount += 1
                player_num = playercount
                username::String = username_list[i]
                uberid::String = uberid_list[i]
                if isdigit(uberid[1])
                    player_type = "pa inc"
                elseif (uberid == "-1")
                    player_type = "superstats"
                else
                    player_type = "aiDiff"
                end
                new_army::Army = Army(match_id, player_num, username, player_type, uberid, eco, team_num)
                push!(armies[team_num], new_army)
            end
        end
        
        for team_num in 1:(size(teamIds)[1])
            win::Bool = (match["winner"] == teamIds[team_num])
            shared::Bool = shared_list[team_num]
            size::Int16 = teamsize_list[team_num]
            new_team::Team = Team(match_id, team_num, win, shared, size, armies[team_num])
            push!(teams, new_team)
        end

        

        new_match::Match = Match(match_id, lobbyid, duration, time_start, time_end, titans, ranked, tourney,
            mods, mod_versions, sources, system_name, system_info, server, teams)
        push!(matches, new_match)
    end

    

    sort!(matches)
end

function combine(left::Army, right::Army)::Army
    match_id::Int64 = min(left.match_id, right.match_id)
    player_num::Int16 = left.player_num
    username::String = left.username
    tryleft = tryparse(Int128, left.uberid)
    tryright = tryparse(Int128, right.uberid)
    uberid::String = (
        if (tryleft isa Nothing) # AI user
            left.uberid # left and right should be equal
        elseif (tryleft == -1) # Human user not found
            right.uberid # at best found; at worst the same
        else
            left.uberid
        end
    )
    player_type::String = (
        if (uberid == left.uberid) # choose alongside uberid
            left.player_type
        else
            right.player_type
        end
    )
    eco::Int16 = left.eco
    team_num::Int16 = left.team_num

    result::Army = Army(match_id, player_num, username, player_type, uberid, eco, team_num)
end

function combine(left::Team, right::Team)::Team
    match_id::Int64 = min(left.match_id, right.match_id)
    team_num::Int16 = left.team_num
    win::Bool = max(left.win, right.win)
    shared::Bool = left.shared
    tsize::Int32 = left.size
    armies::Vector{Army} = Vector{Army}()

    for i in 1:size(left.armies, 1)
        push!(armies, combine(left.armies[i], right.armies[i]))
    end

    result::Team = Team(match_id, team_num, win, shared, tsize, armies)
end

function combine(left::Match, right::Match)::Match
    match_id::Int64 = min(left.match_id, right.match_id)
    lobbyid::Int64 = left.lobbyid
    time_start::Int32 = min(left.time_start, right.time_start)
    time_end::Union{Int32,String} = ( if ( typeof(left.time_end) == typeof(right.time_end) == String ) 
                            max(left.time_end, right.time_end)
                        elseif (typeof(left.time_end) == String)
                            left.time_end
                        elseif (typeof(right.time_end) == String)
                            right.time_end
                        else
                            "NULL"
                        end
                    )
    duration::String = (if (typeof(time_end) == Int32) string(time_end - time_start) else "NULL" end)
    titans::Bool = left.titans || right.titans
    ranked::Bool = left.ranked || right.ranked
    tourney::Bool = left.tourney || right.tourney
    mods::String = left.mods
    mod_versions::String = left.mods
    sources::String = left.sources
    system_name::String = left.system_name
    system_info::String = left.system_info
    server::String = left.server
    
    teams::Vector{Team} = Vector{Team}()

    for i in 1:(size(left.teams)[1])
        push!(teams, combine(left.teams[i], right.teams[i]))
    end

    result::Match = Match(match_id, lobbyid, duration, time_start, time_end, titans, 
            ranked, tourney, mods, mod_versions, sources, 
            system_name, system_info, server, teams)
end

function insert_match(match::Match, conn::LibPQ.Connection)::Nothing
    # print("insert match $(match.match_id)\n")
    query::String = 
    "   INSERT INTO reckoner.matches(
        match_id, lobbyid, duration,
        time_start, time_end, titans,
        ranked, tourney, mods, mod_versions,
        system_name, system_info,
        server, source_superstats)
        VALUES(
        $(match.match_id), $(match.lobbyid),
        $(match.duration), $(match.time_start),
        $(match.time_end), $(match.titans),
        $(match.ranked), $(match.tourney),
        $(match.mods), $(match.mod_versions),
        '$(match.system_name)',
        '$(match.system_info)', '$(match.server)',
        TRUE);    "
    LibPQ.execute(conn, query)

    for team in match.teams
        query = 
        "   INSERT INTO reckoner.teams(   
            match_id, team_num, win,
            shared, size)
            VALUES(   
            $(team.match_id), $(team.team_num),
            $(team.win), $(team.shared), 
            $(team.size));   "
        LibPQ.execute(conn, query)

        for army in team.armies
            query = 
            "   INSERT INTO reckoner.armies(
                match_id, player_num, username,
                player_type, player_id, eco10, team_num)
                VALUES(
                $(army.match_id), $(army.player_num),
                '$(army.username)', '$(army.player_type)', 
                '$(army.uberid)',
                $(army.eco), $(army.team_num));  "
            LibPQ.execute(conn, query)
        end
    end
end

function update_match(match::Match, conn::LibPQ.Connection)::Nothing
    print("update match $(match.match_id)\n")
    query::String = 
    "   UPDATE reckoner.matches
        SET
        lobbyid = $(match.lobbyid), 
        duration = $(match.duration),
        time_start = $(match.time_start), 
        time_end = $(match.time_end), 
        titans = $(match.titans),
        ranked = $(match.ranked),
        tourney = $(match.tourney),
        mods = $(match.mods),
        mod_versions = $(match.mod_versions),
        source_superstats = TRUE,
        system_name = '$(match.system_name)',
        system_info = '$(match.system_info)',
        server = '$(match.server)'
        WHERE 
        match_id = $(match.match_id);"
    LibPQ.execute(conn, query)

    for team in match.teams
        query = 
        "   UPDATE reckoner.teams
            SET
            win = $(team.win),
            shared = $(team.shared),
            size = $(team.size)
            WHERE 
            match_id = $(team.match_id)
            AND team_num = $(team.team_num);"
        LibPQ.execute(conn, query)

        for army in team.armies
            query = 
            "   UPDATE reckoner.armies
                SET
                username = '$(army.username)',
                player_type = '$(army.player_type)',
                player_id = '$(army.uberid)',
                eco10 = $(army.eco),
                team_num = $(army.team_num)
                WHERE 
                match_id = $(army.match_id)
                AND player_num = $(army.player_num);"
            LibPQ.execute(conn, query)
        end
    end
end

function send_to_postgres(matches::Vector{Match}, conn::LibPQ.Connection)::Nothing
    match_ids::Vector{Int64} = [match.match_id for match in matches]
    query::String = "   SELECT match_id
                        FROM reckoner.matches
                        INNER JOIN (
                            SELECT $(pop!(match_ids)) AS temp
                            "
    for match_id in match_ids
        query = query * "   UNION ALL SELECT $(match_id)\n"
    end
    query = query * "   ) AS q ON reckoner.matches.match_id = q.temp
                        ORDER BY match_id;"

    res = LibPQ.execute(conn, query)

    existing = Tables.columntable(res).match_id

    LibPQ.execute(conn, "BEGIN;")

    i::Int64 = 1
    last_match::Union{Match, Nothing} = nothing
    last_match_id::Int64 = 0
    for match in matches
        not_existing::Bool = true
        if size(existing)[1] > 0
            while ((existing[i] < match.match_id) && (i < size(existing)[1]))
                i += 1
            end

            if (-500 < (existing[i] - match.match_id) < 500) # if match already in database before inserts
                not_existing = false
                if (-500 < (match.match_id - last_match_id) < 500) # if match was put in database in last step
                    update_match(combine(match, last_match), conn)
                else                    
                    update_match(match, conn)
                end
            end
        end

        if not_existing
            if (-500 < (match.match_id - last_match_id) < 500) # if match was put in database in last step
                update_match(combine(match, last_match), conn)
            else # if match not already in database
                insert_match(match, conn)
            end
        end
        last_match_id = match.match_id
        last_match = match
    end

    LibPQ.execute(conn, "COMMIT;")

    # exclusion_set::Set{String} = {match
    nothing
end

function send_to_postgres(matches::Vector{Match})::Nothing
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    try
        send_to_postgres(matches, conn)
    finally
        LibPQ.close(conn)
    end
end

function update_name_history(matches::Vector{Match})::Nothing
    obs::Vector{Observance} = Vector{Observance}()
    for match in matches
        for team in match.teams
            for army in team.armies
                if !(army.uberid isa Nothing)
                    new_obs = Observance(army.player_type, army.uberid, army.username, match.time_start)
                    push!(obs, new_obs)
                    if typeof(match.time_end) != String
                        new_obs_2 = Observance(army.player_type, army.uberid, army.username, match.time_end)
                        push!(obs, new_obs_2)
                    end
                end
            end
        end
    end
    name_hist::NameHist = create_name_hist(obs)

    name_hist_to_postgres(name_hist)

    nothing
end