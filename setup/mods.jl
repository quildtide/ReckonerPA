import CSV
import LibPQ
import Tables
import JSON

print("hallo\n")
conn = LibPQ.Connection("dbname=reckoner user=reckoner")
LibPQ.execute(conn,"DELETE FROM reckoner.mod_aliases")
LibPQ.execute(conn,"DELETE FROM reckoner.mods")

data = CSV.File

open("mods.csv", "r") do f
    global data = CSV.File(f, type = String)
end

print("yey\n")

LibPQ.execute(conn, "BEGIN;")
LibPQ.load!(
    (id = data.mod_id, pen = data.penalty, white = data.whitelist), conn,
    "INSERT INTO reckoner.mods (mod_id, penalty, whitelist)
    VALUES (\$1, \$2, \$3);")

for i in data
    if !ismissing(i.parameters)
        temp = sort(collect(JSON.parse(i.parameters)))
        params = "{"
        vals = "{"
        for j in temp
            params = params * j[1] * ","
            vals = vals * string(j[2]) * ","
        end
        params = params[1:end-1] * "}"
        vals = vals[1:end-1] * "}"
        LibPQ.execute(conn,
            """UPDATE reckoner.mods
                SET parameters = \$2, values = \$3
                WHERE mod_id = \$1""", [i.mod_id, params, vals])
    end       
end

LibPQ.execute(conn, "COMMIT;")

close(conn)