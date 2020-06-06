import Base.Filesystem

include("get_gamefeed_backend.jl")

function import_gamefeed_archive()
    main_dir::String = "static_data_sources/gamefeed_archive/"
    auto_dir::String = main_dir * "auto/"

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    for (i, j, k) in Filesystem.walkdir(auto_dir)
        for m in k
            process_gamefeed(read(open(auto_dir * m)), conn)
        end
    end

    for m in ["1.json", "2.json", "3.json"]
        process_gamefeed(read(open(main_dir * m)), conn)
    end
end