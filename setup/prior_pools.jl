# smurfs.jl and the smurfs table originally handled alternate accounts (smurfs)
# However, many entries nowadays handle duplicate AI identifiers due to various factors
# There are also many AIs that share a lot in common but may have slight differences
# It does not make sense for these AIs to share everything, but it makes sense to pool prior ratings
# Examples are the VV personalities or the QBE variants of personalities

qbe(id::AbstractString) = id * " QBE"

function ex_pool_query(conn, pool_type, pool_id, player_type, player_id)
    LibPQ.execute(conn, "
        INSERT INTO reckoner.prior_pools (
            pool_type, pool_id,
            player_type, player_id
        ) VALUES (
            '$pool_type', '$pool_id',
            '$player_type', '$player_id'
        );")
end

function multipool(conn, pool_id::AbstractString, types::AbstractVector, ids::AbstractVector)
    for (type, id) in zip(types, ids)
        ex_pool_query(conn, "prior_pool", pool_id, type, id)
    end
end

function multipool(conn, pool_id::AbstractString, ids::AbstractVector, inc_qbe::Bool = true)
    multipool(conn, pool_id, ["aiDiff" for i in ids], ids)
    if inc_qbe
        multipool(conn, pool_id, ["aiDiff" for i in ids], qbe.(ids))
    end
end

function pool_qbe(conn, ids::AbstractVector)
    for id in ids
        ex_pool_query(conn, "aiDiff", id, "aiDiff", qbe(id))
    end
end

function submit_prior_pools()
    wondible_qbe_list = [
        "Bot Rush", "Advanced Rush", "Turtle", "Brad Rush",
        "Extreme Low Metal Games", "Land/Naval", "Legonis Machina (land/air)",
        "Foundation (air/naval)", "Synchronous (balanced)", "Revenants (orbital)"
    ]

    aip_list = [
        "aipAggressive", "aipAir", "aipAmphibious", "aipBot", "aipCautious",
        "aipDefender", "aipEconomist", "aipFabber", "aipFastTech", "aipFoundation",
        "aipLand", "aipLegonisMachina", "aipLowTech", "aipNaval", "aipOrbital",
        "aipRevenants", "aipRush", "aipSwarm", "aipTank", "aipTurtle"
    ]

    conn = LibPQ.Connection("dbname=reckoner user=reckoner")
    LibPQ.execute(conn, "BEGIN;")
    LibPQ.execute(conn, "DELETE FROM reckoner.prior_pools;")
    
    ex_pool_query(conn, "aiDiff", "Absurd", "aiDiff", "Absurd HVC")

    multipool(conn, "vv_group_0", ["V_Arcturus_V", "V_Betelgeuse_V", "V_Canopus_V"])

    pool_qbe(conn, wondible_qbe_list)
    pool_qbe(conn, aip_list)

    LibPQ.execute(conn, "COMMIT;")
    close(conn)
end

submit_prior_pools()