import HTTP
import Sockets

const ROUTER = HTTP.Router()

function hallo(req::HTTP.Request)
    print("hallo\n")
    return HTTP.Response(200, "HALLO WARLD")
end

function echo_req(req::HTTP.Request)
    message::String = HTTP.URIs.splitpath(req.target)[2]
    print("hoi\n")

    return HTTP.Response(200, message)
end

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

HTTP.@register(ROUTER, "GET", "/echo/*", echo_req)

HTTP.serve(ROUTER, Sockets.IPv4("64.20.35.179"), 8085)

# HTTP.serve(ROUTER, Sockets.localhost, 8085)