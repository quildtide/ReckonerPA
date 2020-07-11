import HTTP
import Dates
import JSON
import XXhash

include("./reckoner_common.jl")

include("./get_superstats_backend.jl")

function get_superstats(conn)
    last_time = open("last_superstats_update") do infile
        last_time = parse(Int64, read(infile, String))
        (last_time)
    end

    this_time = Int(trunc(time()) * 1000)
    
    req = HTTP.request("GET", 
        "https://flubbateios.com/stats/api/matches/time?min=$last_time&max=$this_time")

    # open("test.json", "w") do outfile
    #     write(outfile, String(req.body))
    # end

    data = JSON.parse(String(req.body))

    if isempty(data)
        return nothing
    end

    matches = reformat_superstats_data(data)

    send_to_postgres(matches, conn)

    update_name_history(matches)

    open("last_superstats_update", "w") do outfile
        write(outfile, string(this_time))
    end
end
