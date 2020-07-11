import CSV
import LibPQ

function import_pre_eval()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    LibPQ.execute(conn, "BEGIN;")

    counter::Int64 = 0
    for row in CSV.Rows("static_data_sources/pre-evaluated csv/match_evaluation_export.csv")
        query::String = "
                UPDATE reckoner.armies
                SET alpha = $(row.alpha),
                    beta = $(row.beta),
                    win_chance = $(row.win_chance)
                WHERE match_id = $(row.match_id)
                AND player_num = $(row.player_num);"

        LibPQ.execute(conn, query)

        counter += 1

        if (counter % 5000) == 0
            LibPQ.execute(conn, "COMMIT;")
            LibPQ.execute(conn, "BEGIN;")
            println("$counter scores updated")
        end
    end

    LibPQ.execute(conn, "COMMIT;")
end

import_pre_eval()