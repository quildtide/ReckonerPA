using Printf

import LibPQ
import Tables

include("aux_utilities.jl")

function simple_player_rating(uberid::UInt64, conn)::String
    player_hist::PlayerHist = get_player_matches(uberid, conn)

    context::PAMatch = PAMatch((win_chance = 0.5, alpha=2.5, beta=1, timestamp=Int(trunc(time())), 
                        win=false, team_id=1, team_size = 1, team_size_mean = 1.0, 
                        team_size_var = 0.0, team_count = 2, match_id = 0, eco = 1.0, 
                        eco_mean = 1.0, eco_var = 0.0, all_dead = false, shared = false, 
                        titans = true, ranked = false, tourney = false, player_num = 1))
    
    # locus::Vector{PAMatch} = gen_locus(context, [1, 1, 1, 1, 1, 2, 2, 2, 2, 2])

    # locus::Vector{PAMatch} = gen_locus(context, [1, 2])

    # past_matches::Vector{PAMatches} = gen_past(locus, first(values(player_hist)))

    # rat::Normal{Float64} = ratings(locus, past_matches, pa_reck)[1]

    # println(ratings(locus, past_matches, pa_reck))

    rat::Normal{Float64} = rating(context, merge(aup(context, pa_reck), first(values(player_hist))), pa_reck)

    # q1::Float64 = mean(rat) - 2 * std(rat)
    # q3::Float64 = mean(rat) + 2 * std(rat)

    return "$(@sprintf("%d", mean(rat))) ± $(@sprintf("%d", 2 * std(rat)))"
end


