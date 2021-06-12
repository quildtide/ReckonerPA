using Reckoner

import LibPQ

include("../reckoner_common.jl")
include("reckonerPA_equations.jl")

const EVAL_LIMIT = 987654321

function evaluate_matches(conn; refresh_view = true, mass_reset = false)
    query::String = "
                    SELECT
                        player_type,
                        player_id,
                        time_start as timestamp,
                        match_id,
                        team_num as team_id,
                        win,
                        team_size,
                        team_size_mean,
                        team_size_var,
                        team_count,
                        eco,
                        eco_mean,
                        eco_var,
                        all_dead,
                        shared,
                        titans,
                        ranked,
                        tourney,
                        win_chance,
                        player_num,
                        rating_sd 
                    FROM reckoner.matchrows
                    WHERE NOT SCORED
                    ORDER BY time_start ASC
                    LIMIT $EVAL_LIMIT;
                    "
    res = LibPQ.execute(conn, query)

    println("Checkpoint 1: unscored matches fetched")

    player_hist::Dict{PlayerId, PAMatches} = Dict()
    game_hist::Dict{Int64, Vector{Tuple{PAMatch, PlayerId}}} = Dict()
    game_order::Vector{Int64} = Vector{Int64}()
    players_seen::Set{PlayerId} = Set{PlayerId}()

    function add_to_player_hist!(pid::PlayerId, game::PAMatch)
        if pid in keys(player_hist)
            push!(player_hist[pid], game)
        else
            player_hist[pid] = PAMatches([game])
        end
    end

    for row in res
        curr::PAMatch = row |> PAMatch
        pid::PlayerId = (row.player_type, row.player_id)
        
        push!(players_seen, pid)

        if row.match_id in keys(game_hist)
            push!(game_hist[row.match_id], (curr, pid))
        else
            game_hist[row.match_id] = [(curr, pid)]
            push!(game_order, row.match_id)
        end
    end
    
    println("Checkpoint 2: unscored matches processed")

    if !mass_reset
        stmt_scored_matches = LibPQ.prepare(conn, "
            SELECT
                player_type,
                player_id,
                time_start as timestamp,
                match_id,
                team_num as team_id,
                win,
                team_size,
                team_size_mean,
                team_size_var,
                team_count,
                eco,
                eco_mean,
                eco_var,
                all_dead,
                shared,
                titans,
                ranked,
                tourney,
                win_chance,
                player_num,
                alpha,
                beta,
                win_chance,
                rating_sd  
            FROM reckoner.matchrows
            WHERE SCORED
            AND (player_type, player_id) = ANY (
            SELECT a, b FROM UNNEST(\$1::VARCHAR[], \$2::VARCHAR[]) t(a,b)
            );"
        )
    end

    res = stmt_scored_matches(first.(players_seen), last.(players_seen))
    
    println("Checkpoint 3: scored matches fetched")

    for row in res
        add_to_player_hist!((row.player_type, row.player_id), PAMatch(row))
    end

    println("Checkpoint 4: scored matches processed")

    for id in game_order
        game = values(game_hist[id])
        curr::Vector{PAMatch} = [i[1] for i in game]
        # if length(game) != round(Int64, curr[1].team_size_mean * curr[1].team_count)
        #     continue
        # end

        if length(unique([i.team_id for i in curr])) < curr[1].team_count
            continue
        end

        if length(unique([i.team_id for i in curr])) < 2
            println("single team: $id")
            continue
        end

        past_matches::Vector{PAMatches} = [PAMatches() for i in curr]
        n::Int64 = length(curr)
        for i in 1:n
            if (game[i][2] in keys(player_hist)) 
                past_matches[i] = player_hist[game[i][2]]
            end
        end
        rats::Vector{Normal{Float64}} = ratings(curr, past_matches, pa_reck)
        challenges::Vector{Normal{Float64}} = pa_reck.eff_challenge(rats, team_id.(curr), eco.(curr))

        curr = [setproperties(curr[i], (alpha = mean(challenges[i]), beta = std(challenges[i]), rating_sd = params(rats[i])[2])) for i in 1:n]
        chances::Vector{Float64} = player_win_chances(curr, past_matches, rats, pa_reck)
        
        finished::Vector{PAMatch} = [setproperties(curr[i], (win_chance = chances[i]))  for i in 1:n]

        for i in 1:n
            add_to_player_hist!(game[i][2], finished[i])
        end
    end

    println("Checkpoint 5: scoring complete")

    stmt_update_rating = LibPQ.prepare(conn, "
        UPDATE reckoner.armies
        SET alpha = \$1,
            beta = \$2,
            win_chance = \$3,
            rating_sd = \$4
        WHERE match_id = \$5
        AND player_num = \$6;"
    )

    LibPQ.execute(conn, "BEGIN;")
    for phist in values(player_hist)
        for row in Tables.rows(phist)
            stmt_update_rating(
                mean(row.challenge), std(row.challenge),
                row.win_chance, row.rating_sd,
                row.match_id, row.player_num
            )
        end
    end
    LibPQ.execute(conn, "COMMIT;")

    println("Checkpoint 6: committed scores")

    if refresh_view
        LibPQ.execute(conn, "REFRESH MATERIALIZED VIEW CONCURRENTLY reckoner.matchrows_mat;")
    end

    println("Checkpoint 7: refreshed materialized view")
end


function evaluate_matches()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")
    evaluate_matches(conn)
end

# function mass_evaluate_matches()
#     conn = LibPQ.Connection("dbname=reckoner user=reckoner")

#     query::String = "SELECT COUNT(*) AS count FROM reckoner.matchrows WHERE NOT SCORED;"
#     size::Int64 = first(LibPQ.execute(conn, query)).count

#     for i in 1:ceil(Int64, size / EVAL_LIMIT)
#         evaluate_matches(conn, refresh_view = false, mass_reset)
#     end
# end

function mass_evaluate_matches(reset::Bool = true)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    if reset
        LibPQ.execute(conn, "UPDATE reckoner.armies SET win_chance = NULL, alpha = NULL, beta = NULL, rating_sd = NULL;")
    end

    evaluate_matches(conn, mass_reset = true)
end
