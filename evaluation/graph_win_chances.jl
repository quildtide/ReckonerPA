using Plots
using Dates
using Printf

include("aux_utilities.jl")

ENV["GKSwstype"]="nul"

function eval_win_chances(uberid::UInt64, time_1::Int64, time_2::Int64, time_gran::Int64, conn)::Array{Beta{Float64}, 2}
    context::PAMatch = PAMatch((win_chance = 0.5, alpha=2.5, beta=2.5, timestamp = time_1, 
                        win=false, team_id=1, team_size = 1, team_size_mean = 1.0, 
                        team_size_var = 0.0, team_count = 2, match_id = 0, eco = 1.0, 
                        eco_mean = 1.0, eco_var = 0.0, all_dead = false, shared = false, 
                        titans = true, ranked = false, tourney = false, player_num = 1))

    matches::PAMatches = first(values(get_player_matches(uberid, conn)))

    time_seq::StepRange{Int64,Int64} = time_1:time_gran:time_2

    chance_seq::StepRangeLen{Float64,Base.TwicePrecision{Float64},Base.TwicePrecision{Float64}} = 0.005:0.005:.995

    c::Float64 = 20.0

    opp_alpha::Vector{Float64} = chance_seq .* c
    opp_beta::Vector{Float64} = c .- opp_alpha

    out::Array{Beta{Float64}, 2} = [
        skill(setproperties(context, (alpha = a, beta = b, timestamp = t)), merge(matches, aup(setproperties(context, (timestamp = t)), pa_reck)), pa_reck) 
            for (a,b) in zip(opp_alpha, opp_beta), t in time_seq
    ]
end

function rescale_x(input::Float64)::String
    @sprintf("%4.0f", display_rank(input))
end

function plot_win_chances(uberid::UInt64, time_1::Int64, time_2::Int64, time_gran::Int64, conn, name::String)
    chances::Array{Beta{Float64}, 2} = eval_win_chances(uberid, time_1, time_2, time_gran, conn)

    low_bound::Array{Float64, 2} = quantile.(chances, .25)
    high_bound::Array{Float64, 2} = quantile.(chances, .75)
    # median_val::Array{Float64, 2} = median.(chances)
    exp_val::Array{Float64, 2} = mean.(chances)
    # alpha_val::Array{Float64, 2} = alpha.(chances)
    # beta_val::Array{Float64, 2} = beta.(chances)

    chance_seq::Vector{Float64} = collect(0.005:0.005:.995)
    time_seq::StepRange{Int64,Int64} = time_1:time_gran:time_2

    anim = Animation()

    for (i, t) in enumerate(time_seq)
        p = plot(chance_seq, exp_val[:, i], 
                title = "$name: $(Date(unix2datetime(t)))", 
                legend = false,
                xformatter = rescale_x,
                ylims = [0.0, 1.0],
                xlims = [0.0, .995])
        # plot!(p, chance_seq, alpha_val[:, i])
        # plot!(p, chance_seq, beta_val[:, i])
        # plot!(p, chance_seq, low_bound[:, i])
        # plot!(p, chance_seq, high_bound[:, i])
        # plot!(p, chance_seq, median_val[:, i])
        frame(anim, p)
    end

    gif(anim, "export/$(name)_$time_1-$time_2.gif", fps = 15);

    return nothing
end

function plot_win_chances(days::Integer, uberid::Integer, name::String)
    time_2::Int64 = trunc(Int64, time())
    time_gran::Int64 = (24 * 60 * 60)
    time_1::Int64 = time_2 - days * time_gran

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    plot_win_chances(UInt64(uberid), time_1, time_2, time_gran, conn, name)
end

function plot_win_chances_full(uberid::Integer, name::String)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    time_1::Int64 = first(LibPQ.execute(conn, "SELECT MIN(time_start) FROM reckoner.matchrows WHERE player_id = '$uberid';")).min
    time_2::Int64 = trunc(Int64, time())
    time_gran::Int64 = (24 * 60 * 60)

    plot_win_chances(UInt64(uberid), time_1, time_2, time_gran, conn, name)
end