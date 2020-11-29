using Setfield
using Distributions
import Tables

using Reckoner

include("utility.jl")
include("glicko_equations.jl")

const DEF_MEAN = 1500
const DEF_STD = 350

struct PAMatch <: AbstractMatch
    win_chance::Float64
    alpha::Float64
    beta::Float64
    timestamp::Int64
    win::Bool
    team_id::Int16
    team_size::Int16
    team_size_mean::Float64
    team_size_var::Float64
    team_count::Int16
    match_id::Int64
    eco::Float64
    eco_mean::Float64
    eco_var::Float64
    all_dead::Bool
    shared::Bool
    titans::Bool
    ranked::Bool
    tourney::Bool
    unknown_eco::Bool
    player_num::Int16
    rating_sd::Float64
end

Reckoner.win_chance(m::PAMatch)::Float64 = m.win_chance
Reckoner.challenge(m::PAMatch)::Normal{Float64} = Normal(m.alpha, m.beta)
Reckoner.timestamp(m::PAMatch)::Int64 = m.timestamp
Reckoner.win(m::PAMatch)::Int16 = (if m.win 2 elseif (m.all_dead && m.team_count == 2) 1 else 0 end)
Reckoner.team_id(m::PAMatch)::Int16 = m.team_id
eco(m::PAMatch)::Float64 = m.eco
eco_mean(m::PAMatch)::Float64 = m.eco_mean

check_bool(input::Bool)::Bool = input

check_bool(input::String)::Bool = (input == "t" || input == "true")

check_bool_f(input)::Bool = check_bool(input)

check_bool_t(input)::Bool = check_bool(input)

check_bool_f(input::Missing)::Bool = false

check_bool_t(input::Missing)::Bool = true
    
function replace_missing(input::T, def::T)::T where {T}
    input
end

function replace_missing(input::Missing, def::T)::T where {T}
    def
end

function replace_missing(input::Number, def::T)::T where {T<:Number}
    convert(typeof(def), input)
end

function PAMatch(inp)::PAMatch
    # PAMatch(Beta(0.5, 0.5), inrow.timestamp, inrow.win, inrow.team_id)
    if (:alpha in propertynames(inp))
        alpha::Float64 = replace_missing(inp.alpha, 0.5)
        beta::Float64 = replace_missing(inp.beta, 0.5)
    elseif (:challenge in propertynames(inp))
        alpha = alpha(inp.challenge)
        beta = beta(inp.challenge)
    else # unscored match; use default
        alpha = 0.5
        beta = 0.5
    end 

    win_chance::Float64 = replace_missing(inp.win_chance, 0.5)
    eco::Float64 = replace_missing(inp.eco, 1.0)
    eco_mean::Float64 = replace_missing(inp.eco_mean, 1.0)
    eco_var::Float64 = replace_missing(inp.eco_var, 0.0)
    rating_sd::Float64 = replace_missing(inp.rating_sd, 350.0)

    unknown_eco::Bool = ismissing(inp.eco) | ismissing(inp.eco_mean) | ismissing(inp.eco_var)

    PAMatch(win_chance, alpha, beta, 
            inp.timestamp, check_bool_f(inp.win),
            inp.team_id, inp.team_size,
            inp.team_size_mean, 
            inp.team_size_var, inp.team_count,
            inp.match_id, eco,
            eco_mean, eco_var,
            check_bool_f(inp.all_dead), check_bool_f(inp.shared),
            check_bool_t(inp.titans), check_bool_f(inp.ranked),
            check_bool_f(inp.tourney), unknown_eco, inp.player_num,
            rating_sd)
end

struct PAMatches <: AbstractMatches
    win_chance::Vector{Float64}
    challenge::Vector{Normal{Float64}}
    timestamp::Vector{Int64}
    win::Vector{Bool}
    team_size::Vector{Int16}
    team_size_mean::Vector{Float64}
    team_size_var::Vector{Float64}
    team_count::Vector{Int16}
    match_id::Vector{Int64}
    eco::Vector{Float64}
    eco_mean::Vector{Float64}
    eco_var::Vector{Float64}
    all_dead::Vector{Bool}
    shared::Vector{Bool}
    titans::Vector{Bool}
    ranked::Vector{Bool}
    tourney::Vector{Bool}
    unknown_eco::Vector{Bool}
    player_num::Vector{Int16}
    rating_sd::Vector{Float64}
end

Reckoner.win_chance(matches::PAMatches) = matches.win_chance
Reckoner.challenge(matches::PAMatches) = matches.challenge
Reckoner.timestamp(matches::PAMatches) = matches.timestamp
Reckoner.win(m::PAMatches)::Vector{Int16} = 2 .* (m.win .& .!(m.all_dead))  + 1 .* (m.all_dead .& (m.team_count .== 2))
eco(m::PAMatches)::Vector{Float64} = m.eco
eco_mean(m::PAMatches)::Vector{Float64} = m.eco_mean


Reckoner.challenge(match::Tables.ColumnsRow{PAMatches}) = match.challenge

function PAMatches(intable)::PAMatches
    cols = Tables.columns(intable)
   
    if !(:challenge in propertynames(cols))
        challenge::Vector{Normal{Float64}} = Normal.(cols.alpha, cols.beta)
    else
        challenge = cols.challenge
    end 

    PAMatches(cols.win_chance, challenge, 
            cols.timestamp, check_bool_f.(cols.win),
            cols.team_size,
            cols.team_size_mean, 
            cols.team_size_var, cols.team_count,
            cols.match_id, cols.eco,
            cols.eco_mean, cols.eco_var,
            check_bool_f.(cols.all_dead), check_bool_f.(cols.shared),
            check_bool_t.(cols.titans), check_bool_f.(cols.ranked),
            check_bool_f.(cols.tourney), cols.unknown_eco, cols.player_num,
            cols.rating_sd)
end

macro blank_arrays(copies::Int64)
    Meta.eval(Meta.parse(("[],"^copies)[1:end-1]))
end

function PAMatches()::PAMatches
    t = @blank_arrays 20
    PAMatches(t...)
end

function Base.push!(t::PAMatches, s::PAMatch)
    props = fieldnames(PAMatches)

    for i in props[collect(props .!= :challenge)]
        push!(t[i], s[i])
    end

    push!(t.challenge, challenge(s))
end

function merge(l::PAMatches, r::PAMatches)::PAMatches
    if !(isempty(l.win) || isempty(r.win))
        l2 = l |> Tables.rowtable
        r2 = r |> Tables.rowtable
        return vcat(l2, r2) |> PAMatches
    elseif isempty(l.win)
        return r
    else
        return l
    end
end

function pa_aup(curr::PAMatch)::PAMatches
    # game_1::PAMatch = setproperties(curr, ( win_chance = (.5), 
    #                                         alpha = 1500, beta = 300, 
    #                                         win = true, unknown_eco = false, 
    #                                         all_dead = false, ranked = false, tourney = false))
    # game_2::PAMatch = setproperties(game_1, (win_chance = .5, win = false))

    # PAMatches([game_1, game_2])

    PAMatches()
end

function cond_recip(val::Float64)::Float64
    if val > 1.0
        return (1.0 / val)
    else
        return val
    end
end

function time_penalty(timestamp_1::Int64, timestamp_2::Int64)::Float64
    # The time penalty is e^(rt) where r is -0.03 and t is in days
    # Thus, a game becomes worth 4% less towards a rank for every day.
    rate::Float64 = -0.02
    time::Float64 = (timestamp_1 - timestamp_2) / (24 * 60 * 60)
    penalty::Float64 = exp(rate * time)
end

function team_penalty(curr::PAMatch, prev)::Float64
    n_curr::Int16 = round(Int16, curr.team_size_mean * curr.team_count)
    n_prev::Int16 = round(Int16, prev.team_size_mean * prev.team_count)

    penalty::Float64 = 1.0
    penalty *= cond_recip(log(curr.team_size + .3) / log(prev.team_size + .3))^0.6
    penalty *= cond_recip(log(curr.team_size_mean + .3) / log(prev.team_size_mean + .3))^0.3
    penalty *= cond_recip(log(curr.team_count) / log(prev.team_count))^0.4
    penalty *= cond_recip(log(n_curr) / log(n_prev))^0.4

    if ((curr.team_size == 1) && (prev.team_size != 1)) penalty *= 0.8 end
    if ((curr.team_count == 2) ⊻ (prev.team_count == 2)) penalty *= 0.8 end

    if (prev.team_size_var > 0) penalty *= (2 / (2 + prev.team_size_var)) end
    if (curr.team_size_var > 0) penalty *= (2 / (2 + curr.team_size_var)) end

    penalty
end

function eco_penalty(curr::PAMatch, prev)::Float64
    penalty::Float64 = 1.0

    penalty *= cond_recip(log(curr.eco + 1.01) / log(prev.eco + 1.01))^1.4
    penalty *= cond_recip(log(curr.eco + 1.01) / log(prev.eco + 1.01))^0.7

    if (prev.eco_var > 0) penalty *= (0.5 / (0.5 + prev.eco_var)) end
    if (curr.eco_var > 0) penalty *= (0.5 / (0.5 + curr.eco_var)) end

    penalty
end

function pa_weight(curr::PAMatch, prev)::Float64

    if (curr.timestamp < prev.timestamp)
        return 0.0 # future games do not count to your rank
    end

    if (prev.all_dead & prev.team_count > 2)
        return 0.0
    end

    weight = time_penalty(curr.timestamp, prev.timestamp)

    weight *= team_penalty(curr, prev)

    weight *= eco_penalty(curr, prev)

    if ((curr.shared) ⊻ (prev.shared)) weight *= 0.6 end

    if ((curr.titans) ⊻ (prev.titans)) weight *= 0.8 end
    
    if (prev.unknown_eco) weight *= 0.75 end

    if (prev.ranked) weight *= 1.5 end

    if (prev.tourney) weight *= 2.0 end

    if isnan(weight) print(curr, "\n") end

    if (weight < 0) print(weight, "\n") end

    weight

end

function pa_challenge_window(curr::AbstractMatch, prev)::Float64
    challenge_1::Normal{Float64} = challenge(curr)
    challenge_2::Normal{Float64} = challenge(prev)

    challenge_diff::Normal{Float64} = Normal(mean(challenge_1) - mean(challenge_2), sqrt(var(challenge_1) + var(challenge_2)))

    cdf(challenge_diff, 0)
end

function pa_skill(wins::Vector{Int16}, weights::Vector{<:Real}, challenge_windows::Vector{<:Real}, prior_a::Real, prior_b::Real)::Beta{Float64}
    a::Float64 = prior_a + sum(weights .* (wins ./ 2.0) .* challenge_windows)
    b::Float64 = prior_b + sum(weights .* (1.0 .- wins ./ 2.0) .* (1.0 .- challenge_windows))

    if (isnan(a) || isnan(b)) print(weights, "\n") end

    if ((a <= 0) || (b <= 0)) print(a, ", ", b, "\n") end

    Beta(a, b)
end



function standardize(rating_center::Float64)
    (rating_center - 1500) / 200
end

function unstandardize(aah::Float64)::Float64
    aah * 200 + 1500
end

function unstandardize(aah::Normal{Float64})::Normal{Float64}
    Normal(unstandardize(mean(aah)), std(aah))
end

function challenge_sum(ratings::Vector{Normal{Float64}}, base::Float64)::Normal{Float64}
    n::Int64 = length(ratings)
    mu::Float64 = log(base, sum(base.^mean.(ratings)))
    sigma::Float64 = rms(std.(ratings))

    Normal(mu, sigma)
end

function soft_log(base::Number, val::Number)::Float64
    if val > 0
        return log(base, val + 1)
    else
        return -log(base, -val + 1)
    end
end

const TRB = 10^(1/3)

function team_ratings(ratings::Vector{Normal{Float64}}, teams::Vector{<:Integer}, eco::Vector{Float64})::Vector{Normal{Float64}}
    m::Int64 = length(unique(teams))

    modded_ratings::Vector{Normal{Float64}} = Normal.(standardize.(mean.(ratings) .+ 800 .* log10.(max.(eco, 0.05))), std.(ratings))

    team_totals::Vector{Normal{Float64}} = [unstandardize(challenge_sum(modded_ratings[teams .== i], TRB)) for i in 1:m]

    team_totals
end

function pa_eff_challenge(ratings::Vector{Normal{Float64}}, teams::Vector{<:Integer}, eco::Vector{Float64})::Vector{Normal{Float64}}
    # Effectively measures the strength of the opponents "minus" the strength of teammates
    n::Int64 = length(ratings)
    
    m::Int64 = length(unique(teams))
    
    # av_sz::Float64 = n / m

    # modded_ratings::Vector{Beta{Float64}} = Beta.(alpha.(ratings) .* sqrt.(eco), beta.(ratings) ./ sqrt.(eco))
    
    modded_ratings::Vector{Normal{Float64}} = Normal.(standardize.(mean.(ratings) .+ 800 .* log10.(max.(eco, 0.05))), std.(ratings))

    team_totals::Vector{Normal{Float64}} = [challenge_sum(modded_ratings[teams .== i], TRB) for i in 1:m]

    challenges::Vector{Normal{Float64}} = Vector{Normal{Float64}}(undef, n)

    for i in 1:n
        opp::Normal{Float64} = challenge_sum(team_totals[1:m .!= teams[i]], 10^(1/2))

        allies::BitArray = (teams .== teams[i]) .& (1:n .!= i)
        ally_n::Int64 = sum(allies)
        mu_ally::Float64 = mean(team_totals[teams[i]]) - standardize(mean(ratings[i]))
        var_ally::Float64 = 
            if ally_n == 0
                0
            else 
                sum(var.(modded_ratings[allies]))
            end

        mu_eff::Float64 = mean(opp) - mu_ally
        sigma_eff::Float64 = sqrt(var(opp) + var_ally)

        challenges[i] = Normal(mu_eff * 200 + 1500, sigma_eff)
    end

    challenges
end

function pa_display_rank(win_chance::Real)::Float64
    win_chance
end



function Reckoner.rating(curr::PAMatch, prev::PAMatches, inst::ReckonerInstance{PAMatch, PAMatches} = reckoner_defaults)::Normal{Float64}

    mu::Float64 = DEF_MEAN

    if !isempty(prev.shared)
        calc_weights::Vector{Float64} = weights(curr, prev, inst)

        d2::Vector{Float64} = glicko_d2.(std.(prev.challenge), prev.win_chance)
    
        rd2::Float64 = 1 / ((1 / DEF_STD^2) + sum(calc_weights ./ d2))

        # mu = mu + log(10) * sum(calc_weights .* glicko_g.(std.(prev.challenge)) .* (win(prev) ./ 2 - prev.win_chance) ./ d2)\

        mu  += sum(calc_weights .* glicko_delta_r.(std.(prev.challenge), prev.win_chance, 1 ./ (1 ./ (DEF_STD.^2) .+  1 ./ d2), win(prev) ./ 2))

        return Normal(mu, sqrt(rd2))
    end

    Normal(mu, DEF_STD)
end

function Reckoner.ratings(curr::Vector{PAMatch}, prev::Vector{PAMatches}, inst::ReckonerInstance{PAMatch, PAMatches} = reckoner_defaults)::Vector{Normal{Float64}}
    rating.(curr, prev, (inst,))
end

function Reckoner.skill(curr::PAMatch, prev::PAMatches, rat::Normal{Float64}, inst::ReckonerInstance{PAMatch, PAMatches} = reckoner_defaults)::Beta{Float64}
    calc_weights::Vector{Float64} = weights(curr, prev, inst)

    calc_windows::Vector{Float64} = challenge_windows(curr, prev, inst)

    c::Float64 = 10.0
    a::Float64 = glicko_expval(mean(rat), curr.alpha, curr.beta) * c
    b::Float64 = c - a

    eps = 0.000001

    a = max(a, eps)
    b = max(b, eps)

    inst.skill(win(prev), calc_weights, calc_windows, a, b)
end

function Reckoner.skill(curr::PAMatch, prev::PAMatches, inst::ReckonerInstance{PAMatch, PAMatches} = reckoner_defaults)::Beta{Float64}
    rat::Normal{Float64} = rating(curr, prev, inst)

    skill(curr, prev, rat, inst)
end

function Reckoner.skills(curr::Vector{PAMatch}, prev::Vector{PAMatches}, rats::Vector{Normal{Float64}}, inst::ReckonerInstance{PAMatch, PAMatches} = reckoner_defaults)::Vector{Beta{Float64}}
    skill.(curr, prev, rats, (inst,))
end

function Reckoner.eff_challenge(curr::Vector{PAMatch}, prev::Vector{PAMatches}, inst::ReckonerInstance{PAMatch, PAMatches} = reckoner_defaults)::Vector{Normal{Float64}}
    benches::Vector{Normal{Float64}} = ratings(curr, prev, inst)

    inst.eff_challenge(benches, team_id.(curr), eco.(curr))
end

function Reckoner.win_chances(curr::Vector{R}, prev::Vector{T}, rats::Vector{Normal{Float64}}, inst::ReckonerInstance{R,T} = reckoner_defaults)::Vector{Beta{Float64}} where {R, T}
    local_skills::Vector{Beta{Float64}} = skills(curr, prev, rats, inst)

    win_chances(local_skills, team_id.(curr), inst)
end

pa_reck = ReckonerInstance{PAMatch, PAMatches}(aup = pa_aup, weight = pa_weight, skill = pa_skill, challenge_window = pa_challenge_window, eff_challenge = pa_eff_challenge)
