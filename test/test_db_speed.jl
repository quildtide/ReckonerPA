import LibPQ
import Tables

function scoring_process()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    for x in 1:5000

        query::String = 
            """ SELECT matches.match_id, uberid, teams.team_num, 
                faction, eco, shared, commanders, titans, ranked,
                tourney, time_start
                FROM reckoner.armies
                INNER JOIN reckoner.teams 
                ON (teams.match_id, teams.team_num)
                = (armies.match_id, armies.team_num)
                INNER JOIN reckoner.matches
                ON (matches.match_id) = (armies.match_id)
                WHERE scored IS NULL
                ORDER BY time_start ASC
                LIMIT 1 OFFSET $(x)
                """

        res = LibPQ.execute(conn, query)

        current = Tables.rowtable(res)

        
        for i in current
            for j in i
                print(j, ", ")
            end
            print('\n')
        end
    end
end

scoring_process