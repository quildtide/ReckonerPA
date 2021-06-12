import CSV
include("rating_history.jl")

function rating_history_csv(player_types, player_ids, eval_times, reckoner_version = DEF_RECKONER_VER, ip_addr = "")
    conn = LibPQ.Connection("dbname=reckoner user=reckoner_reader host=$ip_addr")
    stmt = prepare_player_matches_ps(conn)

    player_hists = get_player_matches(stmt, player_ids, player_types)
    
    rating_table = reshape([
        calc_rating_hist(matches, i, pid[1], pid[2], reckoner_version) for
        i in eval_times, (pid, matches) in player_hists
    ], (:,))

    filename = "./export/rating_hist/$(trunc(Int64, time())).csv"

    CSV.write(filename, rating_table)
end

function rating_history_csv(player_types, player_ids, reckoner_version = DEF_RECKONER_VER, ip_addr = "")
    conn = LibPQ.Connection("dbname=reckoner user=reckoner_reader host=$ip_addr")
    stmt = prepare_player_matches_ps(conn)

    player_hists = get_player_matches(stmt, player_ids, player_types)
    
    rating_table = Vector{RatingHistRow}()
    
    for (pid, matches) in player_hists
        if length(timestamp(matches)) == 0
            continue
        end
        t1 = minimum(timestamp(matches))
        t2::Int64 = trunc(Int64, time())
        tΔ::Int64 = (24 * 60 * 60)

        rating_table = vcat(rating_table, [
            calc_rating_hist(matches, Dates.unix2datetime(i), pid[1], pid[2], reckoner_version) for i in t1:tΔ:t2
        ])
    end

    filename = "./export/rating_hist/$(trunc(Int64, time())).csv"

    CSV.write(filename, rating_table)
end

function rating_history_all_ais(reckoner_version = DEF_RECKONER_VER, ip_addr = "")
    conn = LibPQ.Connection("dbname=reckoner user=reckoner_reader host=$ip_addr")
    res = LibPQ.execute(conn, "SELECT DISTINCT player_id FROM reckoner.matchrows_mat WHERE player_type = 'aiDiff';")

    ai_players = [i.player_id for i in res]

    rating_history_csv(["aiDiff" for i in ai_players], ai_players, reckoner_version, ip_addr)
end

function rating_history_interesting_players(reckoner_version = DEF_RECKONER_VER, ip_addr = "")
    players = [i.uberid for i in CSV.File("./setup/canonical_names.csv", type = String)]

    rating_history_csv(["pa inc" for i in players], players, reckoner_version, ip_addr)
end