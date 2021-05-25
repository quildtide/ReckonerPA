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

function name_hist_to_postgres(input::NameHist, conn)::Nothing
    stmt_select_namehist = LibPQ.prepare(conn, "
        SELECT player_type, player_id, username, times
        FROM reckoner.name_history
        WHERE (player_type, player_id, username) = ANY (
            SELECT a, b, c 
            FROM UNNEST(
                \$1::VARCHAR[], 
                \$2::VARCHAR[], 
                \$3::VARCHAR[]
            ) t(a,b,c)
        );"
    )

    stmt_insert_namehist = LibPQ.prepare(conn, "
        INSERT INTO reckoner.name_history
        (player_type, player_id, username, times)
        VALUES (\$1, \$2, \$3, \$4);"
    )

    stmt_update_namehist = LibPQ.prepare(conn, "
        UPDATE reckoner.name_history
        SET times = \$4
        WHERE player_type = \$1
        AND player_id = \$2
        AND username = \$3;"
    )

    name_pairs::Vector{Key} = collect(keys(input))

    res = stmt_select_namehist([i[1] for i in name_pairs], [i[2] for i in name_pairs], [i[3] for i in name_pairs])

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
            stmt_update_namehist(key[1], key[2], key[3], collect(times))
        else
            stmt_insert_namehist(key[1], key[2], key[3], collect(times))
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
