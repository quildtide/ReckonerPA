include("get_replayfeed.jl")

function get_uberid_list(conn)::Vector{String}
    res = LibPQ.execute(conn, "SELECT DISTINCT(player_id) FROM reckoner.armies WHERE player_type = 'pa inc';")

    [i.player_id for i in res]
end

function get_replayfeed_by_player()
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    uberid_list::Vector{String} = get_uberid_list(conn)

    url = "https://service.planetaryannihilation.net/GameClient/GetReplayList?MaxResults=9999&FilterUberId="

    auth_url = "https://service.planetaryannihilation.net/GC/Authenticate"

    auth_payload = "{\"TitleId\": 4,\"AuthMethod\": \"UberCredentials\",\"UberName\": \"manlebtnureinmal\",\"Password\": \"$password\"}"

    auth_res = HTTP.request("POST", auth_url, [], auth_payload, cookies = true)

    auth_cookie = Dict{String, String}(
        "auth" => JSON.parse(String(auth_res.body))["SessionTicket"])

    for id in uberid_list
        res = HTTP.request("GET", url * id, cookies = auth_cookie)

        replayfeed = JSON.parse(String(res.body))["Games"]

        process_replayfeed(replayfeed, conn)

        sleep(1)
    end
end

get_replayfeed_by_player()