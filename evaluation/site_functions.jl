using Printf

import LibPQ
import Tables

include("aux_utilities.jl")

function simple_player_rating(uberid::UInt64, conn)::String
    player_hist::PlayerHist = get_player_matches(uberid, conn)

    context::PAMatch = PAMatch((win_chance = 0.5, alpha=2.5, beta=2.5, timestamp=Int(trunc(time())), 
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


