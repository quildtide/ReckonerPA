
function rank(win_chance::Float64)
    ccall((:win_chance_to_rank_, "./reckoner_fortran.so"), Float64, (Ref{Float64},), win_chance)
end