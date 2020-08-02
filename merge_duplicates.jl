import LibPQ
import Tables

using DataStructures

include("reckoner_common.jl")

const LobbyId = Int64
const MatchId = Int64
const DIST = 3600

struct Thing
    lobbyid::LobbyId
    match_id::MatchId
    source_superstats::Bool
    source_replayfeed::Bool
    source_recorder::Bool
    time_start::Int32
end

function Thing(row)::Thing
    Thing(row.lobbyid,
        row.match_id,
        row.source_superstats,
        row.source_replayfeed,
        row.source_recorder,
        row.time_start)
end

replayfeed(thing::Thing)::Bool = (thing.source_replayfeed || thing.source_recorder)

function delete_inferior_row(inferior::Thing, conn)::Nothing
    query::String = 
        "   DELETE FROM reckoner.armies
            WHERE match_id = $(inferior.match_id);"
    LibPQ.execute(conn, query)

    query = 
        "   DELETE FROM reckoner.teams
            WHERE match_id = $(inferior.match_id);"
    LibPQ.execute(conn, query)

    query = 
        "   DELETE FROM reckoner.matches
            WHERE match_id = $(inferior.match_id);"
    LibPQ.execute(conn, query)

    nothing
end

function merge_rows(ss::Thing, rf::Thing, conn)::Nothing
    query::String = 
        "   UPDATE reckoner.matches
            SET source_gamefeed = r.source_gamefeed,
                source_recorder = r.source_recorder,
                source_replayfeed = r.source_replayfeed,
                bounty = r.bounty,
                system_info_gamefeed = r.system_info_gamefeed
            FROM (
                SELECT 
                    source_gamefeed,
                    source_recorder,
                    source_replayfeed,
                    bounty,
                    system_info_gamefeed
                FROM reckoner.matches
                WHERE match_id = $(rf.match_id)) r 
            WHERE match_id = $(ss.match_id);"
    LibPQ.execute(conn, query)

    delete_inferior_row(rf, conn)
end

function collapse_rows(l::Thing, r::Thing, conn)::Bool
    row_collapsed::Bool = true
    if (l.source_superstats && replayfeed(l)) 
        delete_inferior_row(r, conn)
    elseif (r.source_superstats && replayfeed(r))
        delete_inferior_row(l, conn)
    elseif (l.source_superstats && replayfeed(r))
        merge_rows(l, r, conn)
    elseif (replayfeed(l) && r.source_superstats)
        merge_rows(r, l, conn)
    elseif (l.source_replayfeed && r.source_recorder)
        delete_inferior_row(r, conn)
    elseif (l.source_recorder && r.source_replayfeed)
        delete_inferior_row(l, conn)
    else
        row_collapsed = false
    end
    
    row_collapsed
end

function merge_duplicates()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    query::String = "
            SELECT match_id, lobbyid, source_superstats, source_replayfeed, source_recorder, time_start
            FROM reckoner.matches 
            WHERE lobbyid IN (
                SELECT lobbyid FROM (
                    SELECT COUNT(*) as c, lobbyid 
                    FROM reckoner.matches 
                    WHERE time_start > 1544764804
                    GROUP BY lobbyid) a WHERE c > 1) 
            AND server = 'pa inc';"

    res = LibPQ.execute(conn, query)

    lobbyid_dict::MultiDict{LobbyId, Thing} = MultiDict{LobbyId, Thing}()

    for row in res
        temp::Thing = Thing(row)

        insert!(lobbyid_dict, temp.lobbyid, temp)
    end

    counter::Int32 = 0

    LibPQ.execute(conn, "BEGIN;")

    for (lobbyid, curr) in lobbyid_dict
        n::Int32 = length(curr)

        for i in 1:(n-1), j in (i + 1):n
            # if -DIST < (curr[i].time_start - curr[j].time_start) < DIST
                if collapse_rows(curr[i], curr[j], conn)
                    counter += 1
                    break
                end
            # end
        end
    end

    LibPQ.execute(conn, "COMMIT;")

    println("$counter rows collapsed")
end