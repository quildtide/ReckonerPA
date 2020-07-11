include("get_superstats.jl")

include("get_replayfeed.jl")


while true
    print(Dates.now(), '\n')

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    try
        get_superstats(conn)
    catch
        print("failed superstats update\n")
        LibPQ.close(conn)
        conn = LibPQ.Connection("dbname=reckoner user=reckoner")
    end

    try
        get_replayfeed(conn)
    catch
        print("failed replayfeed update\n")
        
    end
    
    print("updated\n")

    LibPQ.close(conn)

    sleep(600)
end