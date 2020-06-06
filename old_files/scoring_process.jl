import LibPQ
import Tables

include("scoring_algs.jl")

function scoring_process()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

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
            LIMIT 15
            """

    res = LibPQ.execute(conn, query)

    current = Tables.rowtable(res)

    print(typeof(current))
    # for i in current
    #     for j in i
    #         print(j, ", ")
    #     end
    #     print('\n')
    # end
end

scoring_process()


        