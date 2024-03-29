using Distributions

alpha(dist::Beta{Float64})::Float64 = params(dist)[1]
beta(dist::Beta{Float64})::Float64 = params(dist)[2]

const PlayerId = Tuple{String, String}

function sanitize(input::Any)::String
    string(input)
end

function sanitize(input::String)::String
    out::String = replace(input, "'" => "''")
end

rms(vals) = sqrt(sum(vals .^ 2)) 

update(priors::Vector{<:Beta})::Beta = Beta(sum(alpha.(priors)), sum(beta.(priors)))

update(priors::Vector{<:Normal})::Normal = Normal(sum(mean.(priors)), sqrt(sum(var.(priors))))

