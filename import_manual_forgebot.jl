import CSV
import LibPQ

include("reckoner_common.jl")

conn = LibPQ.Connection("dbname=reckoner user=reckoner")

for i in CSV.File("manual_data_sources/forgebot.csv")
    query::String = "
            UPDATE reckoner.armies
            SET player_type = 'Forgebot',
            player_id = 'Forgebot'
            WHERE player_id = '651820510498465564'
            AND match_id = (
                SELECT match_id FROM reckoner.matches
                WHERE lobbyid = $(lobbyid_transform(i.lobbyid))
                );"

    LibPQ.execute(conn, query)

    query = "
            UPDATE reckoner.matches
            SET source_corrections = TRUE
            WHERE lobbyid = $(lobbyid_transform(i.lobbyid));"
    LibPQ.execute(conn, query)    
end

