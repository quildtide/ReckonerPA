import XXhash
import Dates

const Timestamp = Int32
const Uberid = String
const Username = String

function sanitize(input::String)::String
    out::String = replace(input, "'" => "''")
end

function sanitize(input::Bool)::String
    out::String = "-1"
end

function format_array_postgres(input::Vector{})::String
    out = "'{"
    for i in input
        out = out * "$(i),"
    end
    if length(out) > 2
        out = out[1:end-1] * "}'"
    else
        out = "'{}'"
    end
end

function format_array_postgres(input::Vector{String})::String
    out = "ARRAY["
    for i in input
        out = out * "'$(i)',"
    end
    if length(out) > 6
        out = out[1:end-1] * "]"
    else
        out = "'{}'"
    end
end

function query_vector_postgres(input::Vector{String})::String
    output = ""
    for i in input
        output = output * "'$(sanitize(i))', "
    end

    output = output[1:end-2]
end

function unsigned_to_signed(input::UInt64)::Int64
    out = input - 9223372036854775808
end

function signed_to_unsigned(input::Int64)::UInt64
    out = input + 9223372036854775808
end

function lobbyid_transformation(input::UInt64)::Int64
    out = unsigned_to_signed(input)
end

function lobbyid_transformation(input::String)::Int64
    temp = tryparse(UInt64, input)
    if (temp isa Nothing)
        temp = XXhash.xxh64(input)
    end
    out = lobbyid_transformation(temp)
end

function lobbyid_transform(input::String)::Int64
    lobbyid_transformation(input)
end

function uberid_transformation(input::UInt64)::Int64
    out = unsigned_to_signed(input)
end

function uberid_transformation(input::String)::Int64
    temp = tryparse(UInt64, input)
    if (temp isa Nothing)
        temp = XXhash.xxh64(input)
    end
    out = uberid_transformation(temp)
end

function match_id_generation(timestamp::Int32,player_names::Vector{String},
                server::String, original_lobbyid::String)::Int64
    if (server == "uber" || server == "pa inc")
        key::String = string(parse(UInt64, original_lobbyid), base = 16, pad = 16)[9:16]
    elseif (server == "river")
        key = string(parse(UInt64, original_lobbyid), base = 16, pad = 16)[1:8]
    else
        key = string(XXhash.xxh32(string(sort(player_names))), base = 16, pad = 8)
    end
    stamp::String = string(timestamp, base = 16, pad = 8)

    match_id::Int64 = unsigned_to_signed(parse(UInt64, (key * stamp), base = 16))
end

function match_id_generation(timestamp::Int32,player_names::Vector{String},
    server::String, lobbyid::Int64)::Int64
    if (server == "uber" || server == "pa inc")
        key::String = string(signed_to_unsigned(lobbyid), base = 16, pad = 16)[9:16]
    elseif (server == "river")
        key = string(signed_to_unsigned(lobbyid), base = 16, pad = 16)[1:8]
    else
        key = string(XXhash.xxh32(string(sort(player_names))), base = 16, pad = 8)
    end
    stamp::String = string(timestamp, base = 16, pad = 8)

    match_id::Int64 = unsigned_to_signed(parse(UInt64, (key * stamp), base = 16))
end

function generate_match_id(timestamp::Timestamp, lobbyid::Int64)::Int64
    match_id_generation(timestamp, ["",""], "pa inc", lobbyid)
end

function eco_transformation(input::Number)::Int16
    out = trunc(10 * input)
end

function display_rank(win_chance::Float64)::Float64
    #=  This function is written in Fortran. 
        This function turns a benchmark win chance (what is used internally in Reckoner)
        into a "display rank" that is usually in the magnitude of 4 digits.
        =#
    ccall((:win_chance_to_rank_, "./reckoner_fortran.so"), Float64, (Ref{Float64},), win_chance)
end
