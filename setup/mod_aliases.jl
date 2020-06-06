import CSV
import LibPQ
import Tables
print("hallo\n")
conn = LibPQ.Connection("dbname=reckoner user=reckoner")
LibPQ.execute(conn,"DELETE FROM reckoner.mod_aliases")

data = CSV.File

open("mod_aliases.csv", "r") do f
    global data = CSV.File(f, type = String)
end

print(data.alt_mod_id)
print(data.main_mod_id)

LibPQ.execute(conn, "BEGIN;")
LibPQ.load!(
    (alts = data.alt_mod_id, mains = data.main_mod_id), conn,
    "INSERT INTO reckoner.mod_aliases (alt_mod_id, main_mod_id)
    VALUES (\$1, \$2);")
LibPQ.execute(conn, "COMMIT;")

close(conn)