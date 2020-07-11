using DataStructures
using Tables

include("./reckoner_common.jl")

struct Observance
    player_type::String
    uberid::Uberid
    username::Username
    time::Timestamp
end

const Key = Tuple{String, Uberid, Username}
const NameHist = Dict{Key, SortedSet{Timestamp}}

function create_name_hist(input::Vector{Observance})::NameHist
    output::NameHist = NameHist()
    for obs in input
        if (obs.uberid, obs.username) in keys(output)
            push!(output[(obs.player_type, obs.uberid, obs.username)], obs.time)
        else
            output[(obs.player_type, obs.uberid, obs.username)] = SortedSet{Timestamp}(obs.time)
        end
    end

    output
end

function merge_name_hist(left::NameHist, right::NameHist)::NameHist
    left_keys::Set{Key} = keys(left)

    for (key::Key, timestamps::SortedSet{Timestamp}) in right
        if key in left_keys
            for i in timestamps
                push!(left[key], i)
            end
        else
            left[key] = timestamps
        end
    end

    left
end

function insert_namehist(key::Key, times::Vector{Timestamp}, conn::LibPQ.Connection)::Nothing
    query::String = "   INSERT INTO reckoner.name_history
                        (player_type, player_id, username, times)
                        VALUES ('$(key[1])', '$(sanitize(key[2]))', '$(sanitize(key[3]))', $(format_array_postgres(times)));"

    LibPQ.execute(conn, query)
        
    nothing
end

function update_namehist(key::Key, times::Vector{Timestamp}, conn::LibPQ.Connection)::Nothing
    query::String = "   UPDATE reckoner.name_history
                        SET times = $(format_array_postgres(times))
                        WHERE player_type = '$(key[1])'
                        AND player_id = '$(sanitize(key[2]))'
                        AND username = '$(sanitize(key[3]))';"

    LibPQ.execute(conn, query)

    nothing
end

function name_hist_to_postgres(input::NameHist, conn)::Nothing
    name_pairs::Vector{Key} = collect(keys(input))

    query::String = "   SELECT player_type, player_id, username, times
                        FROM reckoner.name_history
                        WHERE "
    for pair in name_pairs
        query = query * "(player_type = '$(pair[1])' AND player_id = '$(sanitize(pair[2]))' AND username = '$(sanitize(pair[3]))') OR "
    end
    query = query[1:end-4] * ";"

    res = LibPQ.execute(conn, query)

    table_res = Tables.rowtable(res)

    LibPQ.execute(conn, "BEGIN;")

    # if (length(table_res) == 0)
    #     for (key, times) in input
    #         insert_namehist(key, collect(times), conn)
    #     end
    #     LibPQ.execute(conn, "COMMIT;")

    #     return nothing
    # end

    old_name_hist::NameHist = NameHist((i.player_type, i.player_id, i.username) => SortedSet{Int32}(i.times) for i in table_res)
    name_hist::NameHist = merge_name_hist(input, old_name_hist)
    existing::Set{Key}= keys(old_name_hist)

    for (key, times) in name_hist
        if key in existing
            update_namehist(key, collect(times), conn)
        else
            insert_namehist(key, collect(times), conn)
        end
    end

    LibPQ.execute(conn, "COMMIT;")

    nothing
end


function name_hist_to_postgres(input::NameHist)::Nothing
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    try 
        name_hist_to_postgres(input, conn)
    finally
        LibPQ.close(conn)
    end
end
