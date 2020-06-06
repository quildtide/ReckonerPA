using Distributions
import LibPQ

struct Match_Metadata
    self_team_size::Int16
    mean_team_size::Float64
    team_size_var::Float64
    team_count::Int16
    faction::Union{Nothing, Char}
    ranked::Bool
    tourney::Bool
end


function calc_multiplier(curr::Match_Metadata, match::Match_Metadata)::Float64
    self_team_size_factor::Float64 = match.self_team_size / curr.self_team_size
    if (self_team_size_factor > 1)
        self_team_size_factor = 1 / self_team_size_factor
    end

    mean_team_size_factor::Float64 = match.mean_team_size / curr.mean_team_size
    if (mean_team_size_factor > 1)
        mean_team_size_factor = 1 / mean_team_size_factor
    end

    match_team_size_var_factor::Float64 = 1 / (1 + match.team_size_var / 2)
    curr_team_size_var_factor::Float64 = 1 / (1 + curr.team_size_var / 2)

    team_count_factor::Float64 = match.team_count / curr.team_count
    if (team_count_factor > 1)
        team_count_factor = 1 / (team_count_factor)
    end

    faction_factor::Float64 = 1.0 ? (match.faction) == (curr.faction) : .85

    ranked_factor::Float64 = 1.25 ? match.ranked : 1.0

    tourney_factor::Float64 = 1.5 ? match.tourney : 1.0

    total_multiplier = self_team_size_factor^(1/3) * mean_team_size_factor^(1/3) *
        team_count_factor^(1/3) * match_team_size_var_factor * curr_team_size_var_factor *
        faction_factor * ranked_factor * tourney_factor
end

struct Game
    win::Bool
    score::Float64
    std::Float64
    multiplier::Float64
    timestamp::Int32
end

function Game(win::Bool, score::Float64, std::Float64, 
        metadata::Match_Metadata, curr::Match_Metadata,
        timestamp::Int32)::Game
    Game(win, score, std, calc_multiplier(metadata, curr), timestamp)
end

struct Player
    games::Vector{Game}
    team::Int16
    benchmark::Float64
end

function now()::Int32
    Int32(trunc(time()))
end


function default_games()::Vector{Game}
    [Game(false, 0.5, 0.5, 1, now()), 
     Game(true, 0.5, 0.5, 1, now())]
end

function calc_distance(game::Game, locus::Float64)::Float64
    distrib::Normal = Normal(game.score, game.std)
    distance::Float64 = 0

    if game.win
        distance = 1 - cdf(distrib, locus)
    else
        distance = cdf(distrib, locus)
    end
    distance
end

function time_penalty(game::Game, timestamp::Int32)::Float64
    # The time penalty is e^(rt) where r is -0.02 and t is in days
    # Thus, a game becomes worth 2% less towards a rank for every day.
    rate::Float64 = -0.02
    time::Float64 = (time() - timestamp) / (24 * 60 * 60)
    penalty::Float64 = exp(rate * time)
end


function rank(games::Vector{Game}, locus::Float64)::Float64
    # This is the "rank function"
    # takes in a player's game history as input alongside the benchmark rank
    # of another player; the output is the predicted chance of the player
    # winning versus another player of that benchmark.
    distances::Vector{Float64} = calc_distance.(games, locus)
    main::Vector{Float64} = games.multiplier .* distances
    numerator::Float64 = sum(main[games.win == true])
    denominator::Float64 = sum(main)
    win_chance = numerator / denominator
end

function benchmark(games::Vector{Game})::Float64
    # This generates a player's "benchmark", which is their predicted chance
    # of winning versus an "average" player.
    rank(games, 0.5)
end

function rank(player::Player, locus::Float64)::Float64
    rank(player.games, locus)
end

function benchmark(player::Player)::Float64
    benchmark(player.games)
end

function Player(games::Vector{Game}, team::Int16)::Player
    Player(games, team, benchmark(games))
end

function win_chances(players::Vector{Player})::Vector{Float64}
    # A team's chance of winning is calculated in the following method:
    # The team's base chance to win is the sum of the base chances of its members
    #
    # An individual player's base chance to win is the sum of their chance to win 
    # vs each enemy team
    #
    # An individual player's base chance to win versus a particular enemy team is
    # the product of the player's chance to win versus each member of that enemy team.
    #
    # The total base chances must be normalized to add up to 1.
    team_count::Int16 = max([player.team for player in players])
    chances::Array{Float64, 2} = zeros(Float64, size(players)[1], team_count)
    for (i, player) in enumerate(players)
        for other in [players[players.team .!= player.team]]
            if chances[i, other.team] == 0
                chances[i, other.team] = rank(player.games, other.benchmark)
            else
                chances[i, other.team] *= rank(player.games, other.benchmark)
            end
        end
    end
    sum::Float64 = sum(chances)
    weighted::Vector{Float64} = sum(chances, 2) ./ sum

    teamwide::Vector{Float64} = zeros(Float64, team_count)
    for team in [1:team_count]
        teamwide[team] = sum(weighted[players.team .= team])
    end
    teamwide
end

function team_benchmark(players::Vector{Player})::Float64
    # A team's effective benchmark rank is calculated in the following way:
    # Numerator: sum of all benchmarks on team
    # Denominator: numerator + the product of their chances 
    # to lose versus an "average" player
    #
    # This is similar to the combined win chance formula, but versus a single
    # enemy team composed of a single theoretical "average" player.

    benchmarks::Vector{Float64} = benchmark.(players)

    numerator::Float64 = sum(benchmarks)
    denominator::Float64 = numerator + prod(1 .- benchmarks)

    effective_team_benchmark::Float64 = numerator / denominator
end

function effective_scores(players::Vector{Player})::Float64
    #= This calculates the "effective score" of the opponents/teammates
    that a player faces.

    The algorithm calculates the player's team's chance of losing, if
    the player were replaced by a default-strength player.

    This represents the level of challenge a player faced in a team game.
    =#
    scores::Vector{Float64} = zeros(Float64, size(players)[1]. 1)
    for (i, player) in enumerate(players)
        temp = copy(players)
        temp[i] = Player(default_games(), player.team)
        scores[i] = 1 - win_chances(temp)[player.team]
    end
    scores
end



function Game(row::NamedTuple{}, conn::LibPQ.Connection)::Game
    self_team_size::Int16 = row.commanders
    mean_team_size::Float64 = 1
    team_size_var::Float64 = 0.0
    team_count::Int16 = 2
    faction::Union{Nothing, Char} = row.faction
    ranked::Bool = row.ranked
    tourney::Bool = row.tourney
end

function player_hist(uberids::Vector{String}, conn::LibPQ.Connection)::Vector{Vector{Game}}
    query::String = 
    """ SELECT matches.match_id, uberid, teams.team_num, 
        faction, eco, shared, commanders, titans, ranked,
        tourney, time_start
        FROM reckoner.armies
        INNER JOIN reckoner.teams 
        ON (teams.match_id, teams.team_num)
        = (armies.match_id, armies.team_num)
        INNER JOIN reckoner.matches
        ON (matches.match_id) = (armies.match_id)
        WHERE scored = TRUE
        """

    res = LibPQ.execute(conn, query)

    current = Tables.rowtable(res)

    for i in current
        for j in i
            print(j, ", ")
        end
        print('\n')
    end
end   

