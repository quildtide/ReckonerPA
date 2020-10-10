using Plots
using Dates
using Printf
using Measures

import LibPQ

include("aux_utilities.jl")

ENV["GKSwstype"]="nul"

const LB = -500
const UB = 5000

function eval_win_chances(context::PAMatch, matches::PAMatches, time_seq::StepRange{Int64, Int64}, rank_seq::StepRange{Int64, Int64})::Array{Beta{Float64}, 2}
    out::Array{Beta{Float64}, 2} = [
        skill(setproperties(context, (alpha = a, beta = 1, timestamp = t)), matches, pa_reck) 
            for a in rank_seq, t in time_seq
    ]
end

function eval_ratings(context::PAMatch, matches::PAMatches, time_seq::StepRange{Int64, Int64})::Vector{Normal{Float64}}
    out::Vector{Normal{Float64}} = [
        rating(setproperties(context, (timestamp = t)), matches, pa_reck)
            for t in time_seq
    ]
end

function eval_match_weights(context::PAMatch, matches::PAMatches, time_seq::StepRange{Int64, Int64})::Vector{Vector{Float64}}
    out::Vector{Vector{Float64}} = [
        weights(setproperties(context, (timestamp = t)), matches, pa_reck)
            for t in time_seq
    ]
end

function eval_player(uberid::Any, time_1::Int64, time_2::Int64, time_gran::Int64, conn, player_type::String = "pa inc"
    )::Tuple{Vector{Normal{Float64}}, Array{Beta{Float64}, 2}, Vector{Vector{Float64}}, PAMatches}
    team_size::Int32 = 5
    context::PAMatch = PAMatch((win_chance = 0.5, alpha=2.5, beta = 1, timestamp = time_1, 
                        win=false, team_id=1, team_size = team_size, team_size_mean = team_size, 
                        team_size_var = 0.0, team_count = 2, match_id = 0, eco = 1.0, 
                        eco_mean = 1.0, eco_var = 0.0, all_dead = false, shared = false, 
                        titans = true, ranked = false, tourney = false, player_num = 1))

    matches::PAMatches = first(values(get_player_matches(uberid, conn, player_type)))

    time_seq::StepRange{Int64,Int64} = time_1:time_gran:time_2

    rank_seq::StepRange{Int64,Int64} = LB:50:UB

    match_weights::Vector{Vector{Float64}} = eval_match_weights(context, matches, time_seq)
    ratings::Vector{Normal{Float64}} = eval_ratings(context, matches, time_seq)
    winchances::Array{Beta{Float64}, 2} = eval_win_chances(context, matches, time_seq, rank_seq)
    
    (ratings, winchances, match_weights, matches)
end

function plot_win_chances(uberid::Any, time_1::Int64, time_2::Int64, time_gran::Int64, conn, name::String, player_type::String = "pa inc")
    (rats::Vector{Normal{Float64}}, chances::Array{Beta{Float64}, 2}, match_weights::Vector{Vector{Float64}}, matches::PAMatches) = 
        eval_player(uberid, time_1, time_2, time_gran, conn, player_type)
    
    wins::Vector{Float64} = win(matches) ./ 2
    loci::Vector{Float64} = mean.(matches.challenge)
    exp_val::Array{Float64, 2} = mean.(chances)
    low_bound::Array{Float64, 2} = max.(exp_val .- quantile.(chances, .05), 0)
    high_bound::Array{Float64, 2} = max.(quantile.(chances, .95) .- exp_val, 0)

    rank_seq::Vector{Float64} = collect(LB:50:UB)
    time_seq::StepRange{Int64,Int64} = time_1:time_gran:time_2

    rat_mean::Vector{Float64} = mean.(rats)
    rat_span::Vector{Float64} = 2 .* std.(rats)

    anim = Animation()

    for (i, t) in enumerate(time_seq)
        p = plot(rank_seq, exp_val[:, i], 
                title = "$name: $(Date(unix2datetime(t)))\n($(@sprintf("%d", rat_mean[i])) ± $(@sprintf("%d", rat_span[i])))", 
                xlims = [LB, UB],
                ylims = [0.0, 1.0],
                ribbon = (low_bound[:, i], high_bound[:, i]),
                label = "Expected Win Chance",
                legend = (0.10, 0.10),
                size = (1600, 900),
                xguide = "Effective Challenge Faced (Opponent Rating)",
                yguide = "Expected Win Chance",
                left_margin = 8.0 * mm)
        scatter!(p, [rat_mean[i]], [.5], markersize = 8, label = "Rating", markerstrokewidth = 0)
        scatter!(p, loci, wins, 
                    markersize = max.(8 .* sqrt.(match_weights[i]), 1), 
                    markeralpha = min.(match_weights[i], 1.0),
                    label = "Observed Outcomes", 
                    markerstrokewidth = 0)
        frame(anim, p)
    end

    gif(anim, "export/$(name)_$time_1-$time_2.gif", fps = 15);

    return nothing
end

function plot_win_chances(days::Integer, uberid::Any, name::String, player_type::String = "pa inc")
    time_2::Int64 = trunc(Int64, time())
    time_gran::Int64 = (24 * 60 * 60)
    time_1::Int64 = time_2 - days * time_gran

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    plot_win_chances(uberid, time_1, time_2, time_gran, conn, name, player_type)
end

function plot_win_chances(days::Integer, uberid::Integer, name::String)
    time_2::Int64 = trunc(Int64, time())
    time_gran::Int64 = (24 * 60 * 60)
    time_1::Int64 = time_2 - days * time_gran

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    plot_win_chances(UInt64(uberid), time_1, time_2, time_gran, conn, name)
end

function plot_win_chances_full(uberid::Any, name::String, player_type::String = "pa inc")
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    time_1::Int64 = first(LibPQ.execute(conn, "SELECT MIN(time_start) FROM reckoner.matchrows WHERE player_id = '$uberid';")).min
    time_2::Int64 = trunc(Int64, time())
    time_gran::Int64 = (24 * 60 * 60)

    plot_win_chances(uberid, time_1, time_2, time_gran, conn, name, player_type)
end

function plot_win_chances_full(uberid::Integer, name::String)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    time_1::Int64 = first(LibPQ.execute(conn, "SELECT MIN(time_start) FROM reckoner.matchrows WHERE player_id = '$uberid';")).min
    time_2::Int64 = trunc(Int64, time())
    time_gran::Int64 = (24 * 60 * 60)

    plot_win_chances(UInt64(uberid), time_1, time_2, time_gran, conn, name)
end