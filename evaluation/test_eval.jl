function test_exp_val(means::Vector{<:Number}, teams::Vector{Int64}, eco::Vector{<:Number})::Vector{Float64}
    ratings = Normal.(means, 350)

    win_chances = elo_expval.(means, mean.(pa_eff_challenge(ratings, teams, Float64.(eco))))

end