import LibPQ

include("reckoner_common.jl")

function fix_match_ids()
    new_lobbyid = Dict{Int64, Int64}()

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    query = "   SELECT match_id, lobbyid, time_start
                FROM reckoner.matches
                WHERE server = 'pa inc';
                "
    
    for i in LibPQ.execute(conn, query)
        new_lobbyid[i.match_id] = generate_match_id(i.time_start, i.lobbyid)
    end

    LibPQ.execute(conn, "ALTER TABLE reckoner.matches DISABLE TRIGGER ALL;")
    LibPQ.execute(conn, "ALTER TABLE reckoner.teams DISABLE TRIGGER ALL;")
    LibPQ.execute(conn, "ALTER TABLE reckoner.armies DISABLE TRIGGER ALL;")
    LibPQ.execute(conn, "BEGIN;")
    
    for (i, j) in new_lobbyid
        query = "   UPDATE reckoner.matches
                    SET match_id = $j
                    WHERE match_id = $i;
                    "
        LibPQ.execute(conn, query)
        query = "   UPDATE reckoner.teams
                    SET match_id = $j
                    WHERE match_id = $i;
                    "
        LibPQ.execute(conn, query)
        query = "   UPDATE reckoner.armies
                    SET match_id = $j
                    WHERE match_id = $i;
                    "
        LibPQ.execute(conn, query)
    end
    LibPQ.execute(conn, "COMMIT;")
    LibPQ.execute(conn, "ALTER TABLE reckoner.matches ENABLE TRIGGER ALL;")
    LibPQ.execute(conn, "ALTER TABLE reckoner.teams ENABLE TRIGGER ALL;")
    LibPQ.execute(conn, "ALTER TABLE reckoner.armies ENABLE TRIGGER ALL;")
end