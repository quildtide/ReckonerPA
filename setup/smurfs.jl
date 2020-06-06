import CSV
import LibPQ
import Tables
print("hallo\n")
conn = LibPQ.Connection("dbname=reckoner user=reckoner")
LibPQ.execute(conn,"DELETE FROM reckoner.smurfs")

data = CSV.File

open("smurfs.csv", "r") do f
    global data = CSV.File(f, type = String)
end


LibPQ.execute(conn, "BEGIN;")
LibPQ.load!(
    (alts = data.alt_player_id, mains = data.main_player_id,
    alt_types = data.alt_player_type, main_types = data.main_player_type), conn,
    "INSERT INTO reckoner.smurfs (alt_player_id, main_player_id, alt_player_type, main_player_type)
    VALUES (\$1, \$2, \$3, \$4);")
LibPQ.execute(conn, "COMMIT;")

close(conn)