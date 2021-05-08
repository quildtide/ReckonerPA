import HTTP
import LibPQ
import Tables
import JSON
import Dates

include("reckoner_common.jl")

const GAMEFEED_TIME_FORMAT = Dates.DateFormat("yyyy-mm-dd.HH:MM:SS")

function process_gamefeed(gamefeed_input, conn)
    insert_gamefeed_ps = LibPQ.prepare(conn, "
        INSERT INTO reckoner.gamefeed (
            lobbyid, obs_time, ffa,
            mods, bounty, system_name,
            system_info_gamefeed, QBE, 
            sandbox
        ) VALUES (
            \$1, \$2, \$3, \$4, \$5, 
            \$6, \$7, \$8, \$9
        ) ON CONFLICT (lobbyid) DO NOTHING;
    ")

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
                system_info_gamefeed::String = JSON.json(mdata["game"]["system"]["planets"])
                system_name::String = mdata["game"]["system"]["name"]

                mods::Array{String} = mdata["mod_identifiers"]
                QBE = ("com.pa.quitch.AIBugfixEnhancement" in mods || "com.pa.quitch.AIBugfixEnhancement-dev" in mods)

                LibPQ.execute(insert_gamefeed_ps, (
                    lobbyid, obstime, ffa, mods, bounty, 
                    system_name, system_info_gamefeed, QBE, 
                    sandbox
                ))
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
