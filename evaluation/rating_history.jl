import Tables
import Dates
import LibPQ

include("aux_utilities.jl")
include("reckonerPA_equations.jl")

const DEF_RECKONER_VER = "0.9.1"

struct RatingHistRow <: Tables.AbstractRow
    player_type::String
    player_id::String
    time_updated::Dates.DateTime
    reckoner_version::String
    rating_core::Float64
    rating_1v1::Float64
    rating_team::Float64
    rating_ffa::Float64
    rating_multiteam::Float64
    sd_1v1::Float64
    sd_team::Float64
    sd_ffa::Float64
    sd_multiteam::Float64
end

Base.getproperty(m::RatingHistRow, nm::Symbol) = getfield(m, nm)
Base.propertynames(m::RatingHistRow) = fieldnames(typeof(m))
Tables.schema(m::RatingHistRow) = Tables.Schema(propertynames(m), Tuple(typeof.(m)))

function calc_rating_hist(matches::PAMatches, eval_time::Dates.DateTime, player_type::String, player_id::String, reckoner_version = DEF_RECKONER_VER)
    eval_ts = trunc(Int64, Dates.datetime2unix(eval_time))
    context_1v1 = PAMatch((
        win_chance = 0.5, alpha = 1500, beta = 1, timestamp = eval_ts, 
        win = false, team_id = 1, team_size = 1, team_size_mean = 1, 
        team_size_var = 0.0, team_count = 2, match_id = 0, eco = 1.0, 
        eco_mean = 1.0, eco_var = 0.0, all_dead = false, shared = false, 
        titans = true, ranked = false, tourney = false, player_num = 1, rating_sd = 1
    ))

    context_team = setproperties(context_1v1, (team_size = 5, team_size_mean = 5))
    context_ffa = setproperties(context_1v1, (team_count = 10))
    context_multiteam = setproperties(context_1v1, (team_size = 3, team_size_mean = 3, team_count = 4))

    rat = rating.(
        (context_1v1, context_team, context_ffa, context_multiteam),
        (matches,),
        (pa_reck,)
    )

    rating_row = RatingHistRow(
        player_type, player_id, eval_time, reckoner_version, 1500.0,
        mean.(rat)..., std.(rat)...
    )

    return rating_row
end

function calc_rating_hist(player_type::String, player_id::String, eval_time::Dates.DateTime = Dates.now(), reckoner_version = DEF_RECKONER_VER)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner_reader")

    stmt = prepare_player_matches_ps(conn)
    matches::PAMatches = first(values(get_player_matches(stmt, [player_id], [player_type])))

    calc_rating_hist(matches, eval_time, player_type, player_id, reckoner_version)
end

function calc_rating_hist_remote(ip_addr, player_type::String, player_id::String, eval_time::Dates.DateTime = Dates.now(), reckoner_version = DEF_RECKONER_VER)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner_reader host=$ip_addr")

    stmt = prepare_player_matches_ps(conn)
    matches::PAMatches = first(values(get_player_matches(stmt, [player_id], [player_type])))

    calc_rating_hist(matches, eval_time, player_type, player_id, reckoner_version)
end

function calc_rating_hist_remote(ip_addr, player_type::String, player_id::String, eval_times::AbstractVector, reckoner_version = DEF_RECKONER_VER)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner_reader host=$ip_addr")

    stmt = prepare_player_matches_ps(conn)
    matches::PAMatches = first(values(get_player_matches(stmt, [player_id], [player_type])))
    
    [calc_rating_hist(matches, i, player_type, player_id, reckoner_version) for i in eval_times]
end