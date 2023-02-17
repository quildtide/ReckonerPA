import HTTP
import Sockets
import Dates

include("evaluation/site_functions.jl")
include("reckoner_common.jl")

const ROUTER = HTTP.Router()

conn = LibPQ.Connection("dbname=reckoner user=reckoner")

const player_hist_ps = prepare_player_matches_ps(conn)

const STMT_RATING_CACHE = LibPQ.prepare(conn, "
    SELECT * FROM reckoner.last_rating
    WHERE player_type = ANY(\$1)
    AND player_id = ANY(\$2)
")

const STMT_REPORTER = LibPQ.prepare(conn, "
    INSERT INTO reckoner.reporter (
        reporter_name, reporter_id,
        reporter_army_index, send_timestamp, 
        receive_timestamp, lobby_id,
        sandbox, dynamic_alliances, 
        commanders, alliance_id,
        alliance_length, ai
    ) VALUES (
        \$1, \$2, \$3, \$4, \$5, \$6,
        \$7, \$8, \$9, \$10, \$11, \$12
    )
")

function hallo(req::HTTP.Request)
    print("hallo\n")
    return HTTP.Response(200, "HALLO WARLD")
end

function echo_req(req::HTTP.Request)
    message::String = HTTP.URIs.splitpath(req.target)[2]
    print("hoi\n")

    return HTTP.Response(200, message)
end

HTTP.Handlers.register!(ROUTER, "GET", "/echo/*", echo_req)

function reckoner_chan(req::HTTP.Request)
    message::String = 
        "<!DOCTYPE html>
        <body>
        <img src=\"https://i.imgur.com/GYLOIAz.png\" alt=\"ayy lmao\">
        </body>
        </html>"

    return HTTP.Response(200, message)
end

HTTP.Handlers.register!(ROUTER, "GET", "/", reckoner_chan)


function basic_rating(req::HTTP.Request)
    uberid::Union{UInt64, Nothing} = tryparse(UInt64, HTTP.URIs.splitpath(req.target)[2])
    if uberid isa Nothing
        message::String = "INVALID PLAYER"
    else
        message = simple_player_rating(player_hist_ps, uberid)
    end

    return HTTP.Response(200, message)
end

HTTP.Handlers.register!(ROUTER, "GET", "/basic_rating/*", basic_rating)

function full_rating(req::HTTP.Request)
    
    message = full_player_rating(player_hist_ps, HTTP.URIs.queryparams(HTTP.URIs.URI(req.target))["context"])

    # message = JSON.json(Dict("rating_means"=> Dict(8665259548474200874 => "Hello world")))

    return HTTP.Response(200, message)
end

HTTP.Handlers.register!(ROUTER, "GET", "/api/full_rating", full_rating)

function cached_rating(req::HTTP.Request)
    context = JSON.parse(HTTP.URIs.queryparams(HTTP.URIs.URI(req.target))["context"])
    player_types = context["player_types"]
    player_ids = context["player_ids"]

    res = STMT_RATING_CACHE(player_types, player_ids)

    out = Dict("ratings" => Vector())

    for row in res
        player_rating = Dict(
            "player_type" => row.player_type,
            "player_id" => row.player_id,
            "time_updated" => Dates.datetime2unix(row.time_updated),
            "reckoner_version" => row.reckoner_version,
            "rating_core" => row.rating_core,
            "rating_1v1" => row.rating_1v1,
            "rating_team" => row.rating_team,
            "rating_ffa" => row.rating_ffa,
            "rating_multiteam" => row.rating_multiteam,
            "sd_1v1" => row.sd_1v1,
            "sd_team" => row.sd_team,
            "sd_ffa" => row.sd_ffa,
            "sd_multiteam" => row.sd_multiteam
        )
        push!(out["ratings"], player_rating)
    end

    return HTTP.Response(200, JSON.json(out))
end
HTTP.Handlers.register!(ROUTER, "GET", "/api/cached_rating", cached_rating)

function reporter(req::HTTP.Request)
    report = JSON.parse(IOBuffer(HTTP.payload(req)))

    STMT_REPORTER(
        report["reporter_name"],
        report["reporter_id"],
        report["army_index"],
        Dates.unix2datetime(report["report_timestamp"]/1000),
        Dates.now(Dates.UTC),
        report["lobby_id"],
        report["sandbox"],
        report["dynamic_alliances"],
        JSON.json(report["commanders"]),
        report["alliance_id"],
        report["alliance_length"],
        report["ai"]
    )
    return HTTP.Response(200, "kthnxbai")
end
HTTP.Handlers.register!(ROUTER, "POST", "/api/reporter", reporter)

HTTP.serve(ROUTER, Sockets.IPv4("64.20.35.179"), 8085)

# HTTP.serve(ROUTER, Sockets.localhost, 8085)

println("READY")