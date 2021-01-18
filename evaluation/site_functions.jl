using Printf

import LibPQ
import Tables
import JSON

include("aux_utilities.jl")

function simple_player_rating(uberid::UInt64, conn)::String
    player_hist::PlayerHist = get_player_matches(uberid, conn)

    context::PAMatch = PAMatch((win_chance = 0.5, alpha=1500, beta=350, timestamp=Int(trunc(time())), 
                        win=false, team_id=1, team_size = 5, team_size_mean = 5.0, 
                        team_size_var = 0.0, team_count = 2, match_id = 0, eco = 1.0, 
                        eco_mean = 1.0, eco_var = 0.0, all_dead = false, shared = false, 
                        titans = true, ranked = false, tourney = false, player_num = 1, rating_sd = 350.0))
    
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

function gen_teams(team_sizes::Vector{Int16})::Vector{Int16}
    n::Int16 = sum(team_sizes)
    teams::Vector{Int16} = Vector{Int16}(undef, n)
    c::Int16 = 1
    for (i, s) in enumerate(team_sizes)
        teams[c:c+s-1] .= i
        c += s
    end
    teams
end

function full_player_rating(conn,
        player_types::Vector{String}, player_ids::Vector{String},
        team_sizes::Vector{Int16}, ecos::Vector{Float64}, 
        unique_ids::Vector{String}, shared::Vector{Bool},
        titans::Bool)::Dict{String, Any}

    n::Int16 = length(player_ids)
    teams::Vector{Int16} = gen_teams(team_sizes)
    team_count::Int16 = length(team_sizes)
    team_size_mean::Float64 =  n / team_count
    team_size_var::Float64 = var(team_sizes)

    eco_mean::Float64 = mean(ecos)
    eco_var::Float64 = var(ecos)

    base_context::PAMatch = PAMatch((win_chance = 0.5, alpha=1500, beta=350, timestamp=Int(trunc(time())), 
                        win=false, team_id=1, team_size = 5, team_size_mean = team_size_mean, 
                        team_size_var = team_size_var, team_count = team_count, match_id = 0, eco = 1.0, 
                        eco_mean = eco_mean, eco_var = eco_var, all_dead = false, shared = false, 
                        titans = titans, ranked = false, tourney = false, player_num = 1, rating_sd = 350.0))

    indiv_contexts::Vector{PAMatch} = [setproperties(base_context, (team_id = teams[i], 
            team_size = team_sizes[teams[i]], eco = ecos[i], shared = shared[teams[i]])) for i in 1:n]

    player_hists::PlayerHist = get_player_matches(player_ids, conn, player_types)

    prev_matches::Vector{PAMatches} = [player_hists[(player_types[i], player_ids[i])] for i in 1:n]

    rats::Vector{Normal{Float64}} = ratings(indiv_contexts, prev_matches, pa_reck)

    challenges::Vector{Normal{Float64}} = pa_reck.eff_challenge(rats, teams, ecos)

    indiv_contexts = [setproperties(indiv_contexts[i], (alpha = mean(challenges[i]), beta = std(challenges[i]))) for i in 1:n]

    chances::Vector{Float64} = mean.(win_chances(indiv_contexts, prev_matches, rats, pa_reck))

    team_rats::Vector{Normal{Float64}} = team_ratings(rats, teams, ecos)

    out::Dict{String, Any} = Dict{String, Any}()

    
    # out["rating_means"] = Dict(unique_ids[i] => mean(rats[i]) for i in 1:n)
    # out["rating_stds"] = Dict(unique_ids[i] => std(rats[i]) for i in 1:n)
    # out["team_rating_means"] = [mean(team_rats[i]) for i in 1:team_count]
    # out["team_rating_stds"] = [std(team_rats[i]) for i in 1:team_count]
    # out["win_chances"] = chances

    out["team_stats"] = [Dict("team_rating_mean" => mean(team_rats[i]), "team_rating_std" => std(team_rats[i]), "win_chance" => chances[i]) for i in 1:team_count]
    out["player_stats"] = Dict(unique_ids[i] => Dict("rating_mean" => mean(rats[i]), "rating_std" => std(rats[i])) for i in 1:n)

    out
end

function full_player_rating(input_json::String, conn)::String
    input = JSON.parse(input_json)

    JSON.json(full_player_rating(conn, Vector{String}(input["player_types"]), 
            Vector{String}(input["player_ids"]), Vector{Int16}(input["team_sizes"]), 
            Vector{Float64}(input["ecos"]), Vector{String}(input["unique_ids"]), 
            Vector{Bool}(input["shared"]), input["titans"]))
end