using Printf

import LibPQ
import Tables

include("reckonerPA_equations.jl")

const PlayerHist = Dict{PlayerId, PAMatches}

function gen_locus(template::PAMatch, teams::Vector{Int64})::Vector{PAMatch}
    n::Int64 = length(teams)
    output::Vector{PAMatch} = Vector{PAMatch}(undef, n)

    for i in 1:n
        output[i] = @set template.team_id = teams[i]
    end

    output
end

function gen_past(locus::Vector{PAMatch}, prev::PAMatches)::Vector{PAMatches}
    default::PAMatches = aup(locus[1], pa_reck)

    n = length(locus)

    output::Vector{PAMatches} = [default for i in 1:n]

    output[1] = merge(output[1], prev)

    output
end

function empty_matches()::PAMatches 
    PAMatches(
        Vector{Float64}(),
        Vector{Beta{Float64}}(),
        Vector{Int64}(),
        Vector{Bool}(),
        Vector{Int16}(),
        Vector{Float64}(),
        Vector{Float64}(),
        Vector{Int16}(),
        Vector{Int64}(),
        Vector{Float64}(),
        Vector{Float64}(),
        Vector{Float64}(),
        Vector{Bool}(),
        Vector{Bool}(),
        Vector{Bool}(),
        Vector{Bool}(),
        Vector{Bool}(),
        Vector{Bool}(),
        Vector{Int16}())
end

function get_player_matches(uberids::Vector{UInt64}, conn)::PlayerHist
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
            alpha,
            beta
        FROM reckoner.matchrows_mat
        WHERE SCORED
        AND (player_type, player_id) IN ("
    for id in uberids
        query *= "('pa inc','$id'),"
    end
    query = query[1:end-1] * ");"
    res = LibPQ.execute(conn, query)

    player_hist::PlayerHist = PlayerHist()

    for uberid in uberids
        pid::PlayerId = ("pa inc", string(uberid))
        player_hist[pid] = empty_matches()
    end

    for row in res
        pid::PlayerId = (row.player_type, row.player_id)
        if pid in keys(player_hist)
            push!(player_hist[pid], row |> PAMatch)
        else
            player_hist[pid] = PAMatches([PAMatch(row)])
        end
    end
    
    player_hist
end

function get_player_matches(uberid::UInt64, conn)::PlayerHist
   get_player_matches([uberid], conn)
end

function simple_player_rating(uberid::UInt64, conn)::String
    player_hist::PlayerHist = get_player_matches(uberid, conn)

    context::PAMatch = PAMatch((win_chance = 2.5, alpha=2.5, beta=0.5, timestamp=Int(trunc(time())), 
                        win=false, team_id=1, team_size = 5, team_size_mean = 5.0, 
                        team_size_var = 0.0, team_count = 2, match_id = 0, eco = 1.0, 
                        eco_mean = 1.0, eco_var = 0.0, all_dead = false, shared = false, 
                        titans = true, ranked = false, tourney = false, player_num = 1))
    
    locus::Vector{PAMatch} = gen_locus(context, [1, 1, 1, 1, 1, 2, 2, 2, 2, 2])

    past_matches::Vector{PAMatches} = gen_past(locus, first(values(player_hist)))

    rating::Beta{Float64} = ratings(locus, past_matches, pa_reck)[1]

    q1::Float64 = display_rank(quantile(rating, 0.25), pa_reck)
    q3::Float64 = display_rank(quantile(rating, 0.75), pa_reck)

    return "$(@sprintf("%4.0f", q1)) to $(@sprintf("%4.0f", q3))"
end