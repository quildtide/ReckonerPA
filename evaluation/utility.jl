using Distributions

alpha(dist::Beta{Float64})::Float64 = params(dist)[1]
beta(dist::Beta{Float64})::Float64 = params(dist)[2]

const PlayerId = Tuple{String, String}

function sanitize(input::String)::String
    out::String = replace(input, "'" => "''")
end