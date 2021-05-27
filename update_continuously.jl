include("get_superstats.jl")

include("get_replayfeed.jl")

include("evaluation/evaluate_matches.jl")

while true
    try
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
            LibPQ.close(conn)
            conn = LibPQ.Connection("dbname=reckoner user=reckoner")
        end
        
        try
            evaluate_matches(conn)
        catch
            print("failed evaluate matches\n")
        finally
            LibPQ.close(conn)
        end
        print("updated\n")

        sleep(600)
    catch
        sleep(200)
    end
end