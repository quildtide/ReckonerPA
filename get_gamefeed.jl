include("get_gamefeed_backend.jl")

function get_gamefeed()
    url = "https://games.planetaryannihilation.net/"

    res = HTTP.request("GET", url)

    gamefeed = res.body

    process_gamefeed(gamefeed)
end

function auto_get_gamefeed()
    while true
        get_gamefeed()

        sleep(3 * 60)
    end
end

auto_get_gamefeed()