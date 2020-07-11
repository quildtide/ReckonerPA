include("reckoner_common.jl")

const MatchId = Int64
const LobbyId = Int64
const TeamNum = UInt16
const UberId = String
const PastComposite = NamedTuple{(:team_num, :uberid),Tuple{Int64,String}}

const DATEFORMAT = Dates.DateFormat("mm/dd/yyyy HH:MM:SS p")

const DEFAULT_DUR = 24 * 60 * 60


function get_update(lobbyids::Vector{LobbyId}, conn, timestamp_1::Timestamp, timestamp_2::Timestamp)::Dict{Tuple{LobbyId, Username}, PastComposite}
    output = Dict{Tuple{LobbyId, Username}, PastComposite}()

    if !isempty(lobbyids)
        query::String = "   SELECT lobbyid , username, team_num, player_id, player_type
                            FROM reckoner.armies a 
                            INNER JOIN reckoner.matches m
                            ON a.match_id = m.match_id
                            WHERE server = 'pa inc'
                            AND time_start BETWEEN $timestamp_1 AND $timestamp_2
                            AND lobbyid IN ("
        
        for id in lobbyids
            query = query * string(id) * ", "
        end
        
        if length(lobbyids) > 0
            query = query[1:end-2] * ");"
        else
            query = query * ");"
        end

        res = LibPQ.execute(conn, query)
        
        for i in res
            username = i.username
            if i.player_type == "aiDiff" && length(username) > 4
                if username[1:4] == "AI: "
                    username = username[5:end]
                end
            end
            output[(i.lobbyid, username)] = (team_num = i.team_num, uberid = i.player_id)
        end
    end

    output
end

function get_update(lobbyids::Vector{LobbyId}, conn)::Dict{Tuple{LobbyId, Username}, PastComposite}
    now::Timestamp = floor(Timestamp, time())
    get_update(lobbyids, conn, Timestamp(now - (DEFAULT_DUR)), now)
end

function get_ignore(lobbyids::Vector{LobbyId}, conn, timestamp_1::Timestamp, timestamp_2::Timestamp)::Set{LobbyId}
    output = Set{LobbyId}()

    if !isempty(lobbyids)
        query::String = "   SELECT lobbyid
                            FROM reckoner.matches 
                            WHERE (source_replayfeed OR source_recorder)
                            AND time_start BETWEEN $timestamp_1 AND $timestamp_2
                            AND lobbyid IN ("

        # query::String = "   SELECT lobbyid
        # FROM reckoner.matches 
        # WHERE source_recorder
        # AND lobbyid IN ("
        
        for id in lobbyids
            query = query * string(id) * ", "
        end

        if length(lobbyids) > 0
            query = query[1:end-2] * ");"
        else
            query = query * ");"
        end

        res = LibPQ.execute(conn, query)
        
        for i in res
            push!(output, i.lobbyid)
        end
    end

    output
end

function get_ignore(lobbyids::Vector{LobbyId}, conn)::Set{LobbyId}
    now::Timestamp = floor(Timestamp, time())
    get_ignore(lobbyids, conn, Timestamp(now - (DEFAULT_DUR)), now)
end

function get_sandbox(lobbyids::Vector{LobbyId}, conn, timestamp_1::Timestamp, timestamp_2::Timestamp)::Set{LobbyId}
    output = Set{LobbyId}()

    if !isempty(lobbyids)
        query::String = "   SELECT lobbyid
                            FROM reckoner.gamefeed 
                            WHERE sandbox
                            AND obs_time BETWEEN $timestamp_1 AND $timestamp_2
                            AND lobbyid IN ("
        
        for id in lobbyids
            query = query * string(id) * ", "
        end

        if length(lobbyids) > 0
            query = query[1:end-2] * ");"
        else
            query = query * ");"
        end

        res = LibPQ.execute(conn, query)
        
        for i in res
            push!(output, i.lobbyid)
        end
    end

    output
end

function get_sandbox(lobbyids::Vector{LobbyId}, conn)::Set{LobbyId}
    now::Timestamp = floor(Timestamp, time())
    get_sandbox(lobbyids, conn, Timestamp(now - (DEFAULT_DUR)), now)
end

function get_FFA(lobbyids::Vector{LobbyId}, conn, timestamp_1::Timestamp, timestamp_2::Timestamp)::Set{LobbyId}
    output = Set{LobbyId}()

    if !isempty(lobbyids)
        query::String = "   SELECT lobbyid
                            FROM reckoner.gamefeed 
                            WHERE ffa AND NOT sandbox
                            AND obs_time BETWEEN $timestamp_1 AND $timestamp_2
                            AND lobbyid IN ("
        
        for id in lobbyids
            query = query * string(id) * ", "
        end

        if length(lobbyids) > 0
            query = query[1:end-2] * ");"
        else
            query = query * ");"
        end

        res = LibPQ.execute(conn, query)
        
        for i in res
            push!(output, i.lobbyid)
        end

    end
    output
end

function get_FFA(lobbyids::Vector{LobbyId}, conn)::Set{LobbyId}
    now::Timestamp = floor(Timestamp, time())
    get_FFA(lobbyids, conn, Timestamp(now - (DEFAULT_DUR)), now)
end

function get_QBE(lobbyids::Vector{LobbyId}, conn, timestamp_1::Timestamp, timestamp_2::Timestamp)::Set{LobbyId}
    output = Set{LobbyId}()
    
    if !isempty(lobbyids)
        query::String = "   SELECT lobbyid
                            FROM reckoner.gamefeed 
                            WHERE qbe
                            AND obs_time BETWEEN $timestamp_1 AND $timestamp_2
                            AND lobbyid IN ("
        
        for id in lobbyids
            query = query * string(id) * ", "
        end

        if length(lobbyids) > 0
            query = query[1:end-2] * ");"
        else
            query = query * ");"
        end

        res = LibPQ.execute(conn, query)
        
        for i in res
            push!(output, i.lobbyid)
        end
    end

    output
end

function get_QBE(lobbyids::Vector{LobbyId}, conn)::Set{LobbyId}
    now::Timestamp = floor(Timestamp, time())
    get_QBE(lobbyids, conn, Timestamp(now - (DEFAULT_DUR)), now)
end

function push_gamefeed_to_matches(lobbyid::LobbyId, timestamp::Timestamp)::String
    "
    UPDATE reckoner.matches m
    SET system_name = g.system_name,
    system_info_gamefeed = g.system_info_gamefeed,
    mods = g.mods,
    bounty = g.bounty,
    sandbox = g.sandbox,
    source_gamefeed = TRUE
    FROM (
        SELECT system_name, 
        system_info_gamefeed,
        mods,
        bounty,
        sandbox
        FROM reckoner.gamefeed
        WHERE lobbyid = $lobbyid
        AND obs_time BETWEEN $(timestamp - DEFAULT_DUR) AND $(timestamp + DEFAULT_DUR))
    AS g
    WHERE m.lobbyid = $lobbyid
    AND time_start BETWEEN $(timestamp - DEFAULT_DUR) AND $(timestamp + DEFAULT_DUR);
    "
end

function pair_uberids(usernames::Vector{Username}, uberids::Vector{UberId}, now::Timestamp, conn)::Dict{Username, UberId}
    if length(usernames) == 0 || length(uberids) == 0
        return Dict{Username, UberId}()
    end

    if length(usernames) == length(uberids) == 1 # best case scenario
        return Dict{Username, UberId}(usernames[1] => uberids[1])
    end

    output = Dict{Username, UberId}()

    query::String = "   SELECT player_id, username, times
                        FROM reckoner.name_history
                        WHERE player_id IN ($(query_vector_postgres(uberids)))
                        AND username IN ($(query_vector_postgres(usernames)));
                        "

    res = Tables.columntable(LibPQ.execute(conn, query))

    if length(usernames) == length(res.username) == length(unique(res.username)) == length(unique(res.player_id))
        # 1-to-1 correspondence
        return Dict{Username, UberId}(res.username .=> res.player_id)
    end

    temp_set = Set{Tuple{Username, UberId}}()

    for i in 1:length(res.username)
        push!(temp_set, (res.username[i], res.player_id[i]))
    end

    query = "   SELECT uberid, ubername
                FROM reckoner.ubernames
                WHERE uberid IN ($(query_vector_postgres(uberids)))
                AND ubername IN ($(query_vector_postgres(usernames)));
                "

    res = (username = collect(res.username), player_id = collect(res.player_id), times = collect(res.times))
    for i in LibPQ.execute(conn, query)
        if !((i.ubername, i.uberid) in temp_set)
            push!(res.player_id, i.uberid)
            push!(res.username, i.ubername)
            push!(res.times, [0])
        end
    end

    if length(usernames) == length(res.username) == length(unique(res.username)) == length(unique(res.player_id))
        # 1-to-1 correspondence after accounting for ubername
        return Dict{Username, UberId}(res.username .=> res.player_id)
    end

    # This is when it gets complicated

    output::Dict{Username, UberId} = Dict{Username, UberId}()

    username_mapping::Dict{Username, Set{Int16}} = Dict{Username, Set{Int16}}()
    uberid_mapping::Dict{UberId, Set{Int16}} = Dict{UberId, Set{Int16}}()

    [uberid_mapping[i] = Set{Int16}() for i in uberids]
    [username_mapping[i] = Set{Int16}() for i in usernames]
    
    for i in 1:length(res.username)
        push!(username_mapping[res.username[i]], i)
        push!(uberid_mapping[res.player_id[i]], i)
    end

    function edge_length(i::Int16)::Int64
        minimum::Int64 = abs(now - res.times[i][1])
        for time in res.times[i]
            temp::Int64 = abs(now - time)
            if temp <= minimum
                minimum = temp
            else
                break # we are assuming presorted input
            end
        end

        minimum
    end

    function shortest_edge(indices::Vector{Int16})::Int16
        edge_lengths = edge_length.(indices)

        findmin(edge_lengths)[2]
    end

    while !isempty(username_mapping)
        changed::Bool = false
        for i in keys(uberid_mapping)
            if length(uberid_mapping[i]) == 1 
                # only 1 known name for this uberid
                temp_index = collect(uberid_mapping[i])[1]
                temp_username = res.username[temp_index]
                if length(username_mapping[temp_username]) == 1
                    # this is 1-to-1
                    output[temp_username] = i

                    pop!(uberid_mapping, i)
                    pop!(username_mapping, temp_username)

                    changed = true
                end
            end
        end
        if changed continue end

        for i in keys(uberid_mapping)
            if length(uberid_mapping[i]) == 1 
                # only 1 known name for this uberid
                temp_index = collect(uberid_mapping[i])[1]
                temp_username = res.username[temp_index]

                output[temp_username] = i

                pop!(uberid_mapping, i)
                pop!(username_mapping, temp_username)
                
                changed = true
                break
            end
        end
        if changed continue end

        for i in keys(username_mapping)
            if length(username_mapping[i]) == 1 
                # only 1 uberid for this name
                temp_index = collect(username_mapping[i])[1]
                temp_uberid = res.player_id[temp_index]

                output[i] = temp_uberid

                pop!(uberid_mapping, temp_uberid)
                pop!(username_mapping, i)
                
                changed = true
                break
            end
        end
        if changed continue end

        # greedy remove a single point if we get this far

        still_remaining::Vector{Int16} = Vector{Int16}()
        for (i, j) in username_mapping
            still_remaining = vcat(still_remaining, collect(j))
        end

        if length(still_remaining) > 0
            temp_index = shortest_edge(still_remaining)
            temp_uberid = res.player_id[temp_index]
            temp_username = res.username[temp_index]

            output[temp_username] = temp_uberid

            pop!(uberid_mapping, temp_uberid)
            pop!(username_mapping, temp_username)
        else
            # this means we have at least 1 completely unobserved username
            if length(collect(keys(username_mapping))) == 1 == length(collect(keys(uberid_mapping)))
                # 1 unpaired username/uberid combo; last salvageable case
                output[collect(keys(username_mapping))[1]] = collect(keys(uberid_mapping))[1]
            else
                # worst case; unsalvegeable
                for i in keys(username_mapping)
                    output[i] = "-1"
                end
            end

            break # if we get this far
        end

        
    end

    output
end