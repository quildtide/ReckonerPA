import JSON

include("./get_superstats_backend.jl")

data = open("part9.json") do infile
    data = JSON.parse(read(infile, String))
end

print(size(data))
matches = reformat_superstats_data(data)

send_to_postgres(matches)

update_name_history(matches)


