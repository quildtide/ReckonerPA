import HTTP
import Sockets

include("evaluation/site_functions.jl")

const ROUTER = HTTP.Router()

conn = LibPQ.Connection("dbname=reckoner user=reckoner")

const player_hist_ps = prepare_player_matches_ps(conn)

function hallo(req::HTTP.Request)
    print("hallo\n")
    return HTTP.Response(200, "HALLO WARLD")
end



function echo_req(req::HTTP.Request)
    message::String = HTTP.URIs.splitpath(req.target)[2]
    print("hoi\n")

    return HTTP.Response(200, message)
end

HTTP.@register(ROUTER, "GET", "/echo/*", echo_req)

function reckoner_chan(req::HTTP.Request)
    message::String = 
        "<!DOCTYPE html>
        <body>
        <img src=\"https://i.imgur.com/GYLOIAz.png\" alt=\"ayy lmao\">
        </body>
        </html>"

    return HTTP.Response(200, message)
end

HTTP.@register(ROUTER, "GET", "/", reckoner_chan)


function basic_rating(req::HTTP.Request)
    uberid::Union{UInt64, Nothing} = tryparse(UInt64, HTTP.URIs.splitpath(req.target)[2])
    if uberid isa Nothing
        message::String = "INVALID PLAYER"
    else
        message = simple_player_rating(player_hist_ps, uberid)
    end

    return HTTP.Response(200, message)
end

HTTP.@register(ROUTER, "GET", "/basic_rating/*", basic_rating)

function full_rating(req::HTTP.Request)
    
    message = full_player_rating(player_hist_ps, HTTP.URIs.queryparams(HTTP.URIs.URI(req.target))["context"])

    # message = JSON.json(Dict("rating_means"=> Dict(8665259548474200874 => "Hello world")))

    return HTTP.Response(200, message)
end

HTTP.@register(ROUTER, "GET", "/api/full_rating", full_rating)

HTTP.serve(ROUTER, Sockets.IPv4("64.20.35.179"), 8085)

# HTTP.serve(ROUTER, Sockets.localhost, 8085)

println("READY")