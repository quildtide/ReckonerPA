const GLICKO_Q = log(10) / 400

function glicko_g(rd::Number)::Float64
    1 / sqrt(1 + 3 * GLICKO_Q^2 * rd^2 / pi^2)
end

function glicko_d2(rd_j::Number, win_chance::Number)::Float64
    1 / (GLICKO_Q^2 * glicko_g(rd_j)^2 * win_chance * (1 - win_chance))
end

function glicko_delta_r(rd_j::Number, win_chance::Number, curr_rd2::Number, win::Number)::Float64
    GLICKO_Q * curr_rd2 * glicko_g(rd_j) * (win - win_chance)
end

function reckoner_delta_r(rd_j::Number, win_chance::Number, win::Number)::Float64
    GLICKO_Q * glicko_g(rd_j) * (win - win_chance)
end

function glicko_rd2(d2::Vector{T})::Float64 where T <: Number
    1 / sum( 1 ./ (d2))
end

function glicko_expval(r::Number, r_j::Number, rd_j::Number)::Float64
    1 / (1 + 10^(-1 * glicko_g(rd_j) * (r - r_j) / 400))
end

function elo_expval(r::Number, r_j::Number)::Float64
    1 / (1 + 10^(-1 * (r - r_j) / 400))
end

# function cross_entropy(win::Number, win_chance::Number)::Float64
#     (-(win * log(2, win_chance)) + (1 - win) * log(2, 1 - win_chance)) / 2
# end

# function reckoner_delta_r(rd_j::Number, win_chance::Number, curr_rd2::Number, win::Number)::Float64
#     GLICKO_Q * curr_rd2 * glicko_g(rd_j) * cross_entropy(win, win_chance)
# end