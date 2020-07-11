import HTTP
import LibPQ
import Tables
import JSON
import Dates

include("reckoner_common.jl")

const GAMEFEED_TIME_FORMAT = Dates.DateFormat("yyyy-mm-dd.HH:MM:SS")

function process_gamefeed(gamefeed_input, conn)
    gamefeed = JSON.parse(String(gamefeed_input))

    obstime::Timestamp = floor(Timestamp, Dates.datetime2unix(Dates.DateTime(gamefeed["BackendTime"], GAMEFEED_TIME_FORMAT)))

    LibPQ.execute(conn, "BEGIN;")
    for match in gamefeed["Games"]
        if match["GameServerState"] == "playing"
            mdata = JSON.parse(match["TitleData"])

            if mdata["started"]
                lobbyid::Int64 = lobbyid_transform(match["LobbyID"])
                sandbox::Bool = mdata["sandbox"]
                ffa::Bool = (mdata["mode"] == "FreeForAll")
                bounty::Float64 =   if mdata["bounty_mode"]
                                        mdata["bounty_value"]
                                    else
                                        0.0
                                    end
                system_info_gamefeed::String = sanitize(JSON.json(mdata["game"]["system"]["planets"]))
                system_name::String = sanitize(mdata["game"]["system"]["name"])

                mod_ids = mdata["mod_identifiers"]
                QBE = ("com.pa.quitch.AIBugfixEnhancement" in mod_ids || "com.pa.quitch.AIBugfixEnhancement-dev" in mod_ids)
                mods::String = format_array_postgres(sanitize.(mod_ids))

                if sandbox
                    query::String = "   INSERT INTO reckoner.gamefeed
                                        (lobbyid, obs_time, sandbox)
                                        VALUES
                                        ($lobbyid, $obstime, $sandbox)
                                        ON CONFLICT DO NOTHING;
                                        "
                    LibPQ.execute(conn, query)
                else
                    query = "   INSERT INTO reckoner.gamefeed
                                (lobbyid, obs_time, ffa,
                                mods, bounty, system_name,
                                system_info_gamefeed, QBE)
                                VALUES
                                ($lobbyid, $obstime, $ffa,
                                $mods, $bounty, '$system_name',
                                '$system_info_gamefeed', $QBE)
                                ON CONFLICT (lobbyid) DO UPDATE
                                SET bounty = $bounty;
                                "
                    LibPQ.execute(conn, query)
                end
            end
        end
    end
    LibPQ.execute(conn, "COMMIT;")

end

function process_gamefeed(gamefeed_input)
    conn = LibPQ.Connection("dbname=reckoner user=reckoner")

    try
        process_gamefeed(gamefeed_input, conn)
    finally
        LibPQ.close(conn)
    end
end
