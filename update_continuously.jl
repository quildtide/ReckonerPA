include("get_superstats.jl")

include("get_replayfeed.jl")

while true
    print(Dates.now(), '\n')

    try
        get_superstats()
    catch
        print("failed superstats update\n")
    end

    try
        get_replayfeed()
    catch
        print("failed replayfeed update\n")
    end
    
    print("updated\n")

    sleep(600)
end