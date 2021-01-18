## Reckoner 0.0.10:
- Decay 1% per day, including playercount factor
Average entropy: 0.620878050942388
1v1 Average Entropy: 0.632596724776946
5v5 Average Entropy: 0.641756281522399
\> 2 teams Entropy: 0.441184232613582

## Reckoner 0.0.11:
- Decay 3% per day
Average entropy: 0.620921397288601
1v1 Average Entropy: 0.63063931130303
5v5 Average Entropy: 0.641947132785026
\> 2 teams Entropy: 0.449712906748159

## Reckoner 0.0.12
- Revert to linear weighting for ratings
Average entropy: 0.621780055435712
1v1 Average Entropy: 0.631667641172072
5v5 Average Entropy: 0.642231936686314
\> 2 teams Entropy: 0.4505750805362

## Reckoner 0.0.13
- Revert to cross entropy weighting for ratings (compare to 0.0.11)
- Change window function to simple form
Average entropy: 0.619807820601836
1v1 Average Entropy: 0.630005945694926
5v5 Average Entropy: 0.641787469196765
\> 2 teams Entropy: 0.449345829982358
Dissonant 5v5 Entropy: 0.667512976529696
Dissonant 1v1 Entropy: 0.550488578246918
Ferret 1v1 Entropy: 0.664810866194737
Broom 1v1 Entropy: 0.472098269191156
Broom 5v5 Entropy: 0.624152044455139
Bill 5v5 Entropy: 0.652223934624546

## Reckoner 0.1.01:
- Fixed bug in skill function
Average entropy: 0.615653674634707
1v1 Average Entropy: 0.624789264130252
5v5 Average Entropy: 0.640492741021641
\> 2 team Entropy: 0.451729241265783
Dissonant 5v5 Entropy: 0.666072713197931
Dissonant 1v1 Entropy: 0.547712596240822

## Reckoner 0.1.02
- Change default prior from Beta(2.5, 2.5) to Beta(0.5, 0.5)
Average entropy: 0.61561638606324
1v1 Averag Entropy: 0.624816045195975
5v5 Average Entropy: 0.640720300766493
\> 2 team Entropy: 0.452147042390711
Dissonant 5v5 Entropy: 0.666334759838834
Dissonant 1v1 Entropy: 0.55441205209699

## Reckoner 0.1.03
- Change default prior from Beta(0.5, 0.5) to Beta(0.5, 1.0)
Average entropy: 0.615492126890907
1v1 Average Entropy: 0.621453087772406
5v5 Average Entropy: 0.640293880789396
\> 2 team Entropy: 0.451896389963776
Dissonant 5v5 Entropy: 0.668429192071681
Dissonant 1v1 Entropy: 0.553844679638226

## Reckoner 0.01.04
- Revert default prior rating to Beta(0.5, 0.5) *(see 0.1.02)*
- Change the rating of the default opponents to Beta(20, 20) instead of using the same value as the prior.
Average entropy: 0.615612692925373
1v1 Average Entropy: 0.624813382757267
5v5 Average Entropy: 0.640720299765894
\> 2 team Entropy: 0.452147045023299
Dissonant 5v5 Entropy: 0.666334753169925
Dissonant 1v1 Entropy: 0.554412051721413

## Reckoner 0.01.04a
- ATTEMPTED: Once again changing the default rating to Beta(0.5, 1.0)
Average entropy: 0.61560646091599
1v1 Average Entropy: 0.624801428825552
5v5 Average Entropy: 0.640720299638697
\> 2 team Entropy: 0.452147044994677
Dissonant 5v5 Entropy: 0.666334753145501
Dissonant 1v1 Entropy: 0.554412051663649


## Reckoner 0.01.05
- Actually changed the default rating to Beta(0.5, 1.0)
Average entropy: 0.615606250551957
1v1 Average Entropy:
5v5 Average Entropy:
\> 2 team Entropy:
Dissonant 5v5 Entropy: 0.666334753145501
Dissonant 1v1 Entropy: 0.554412051663649

## Reckoner 0.01.05
- Changed the default rating to Beta(0.5, 3.5)
- Realigned the default opponent with the default rating
Average entropy: 0.625443007352506
1v1 Average Entropy: 0.622698289509789
5v5 Average Entropy: 0.64072625810439
\> 2 team Entropy: 0.453220462076455
Dissonant 5v5 Entropy: 0.673946634252594
Dissonant 1v1 Entropy: 0.560479990332671

## Reckoner 0.01.05
- Changed the default rating to Beta(0.5, 1.5)
Average entropy: 0.616969379167673
1v1 Average Entropy: 0.620494362188475
5v5 Average Entropy: 0.640252633496133
\> 2 team Entropy: 0.452062038438219
Dissonant 5v5 Entropy: 0.670011252636235
Dissonant 1v1 Entropy: 0.555131393194243

## Reckoner 0.02.01
- Reverted the default rating to Beta(0.5, 1.0)
- Changed the effective challenge equation
Average entropy: 0.615194514079239
1v1 Average Entropy: 0.621125303087504
5v5 Average Entropy: 0.64022336762451
\> 2 team Entropy: 0.451722407383015
Dissonant 5v5 Entropy: 0.668482928016954
Dissonant 1v1 Entropy: 0.552534607790022

## Reckoner 0.02.02
- Changed ranked/tourney default ratings to align with normal Beta(0.5, 1.0)
Average entropy: 0.61415861398656
1v1 Average Entropy: 0.61997608381115
5v5 Average Entropy: 0.639793356467644
\> 2 team Entropy: 0.451012126952687
Dissonant 5v5 Entropy: 0.668629738506964
Dissonant 1v1 Entropy: 0.546255214631731

## Reckoner 0.03.01
- Changed rating algorithm to account for certainty gained from expected results
- Side effect: ratings update linearly again instead of using cross entropy.
Average entropy: 0.613308317523969
1v1 Average Entropy: 0.62041276077725
5v5 Average Entropy: 0.639906848958364
\> 2 team Entropy: 0.4514647109292
Dissonant 5v5 Entropy: 0.667121959944014
Dissonant 1v1 Entropy: 0.543032636363027

## Reckoner 0.03.01
- Took the square root of weight for ratings

## Reckoner 0.04.01
- Switch to Glicko-style scaling on rating/challenge
Average entropy: 0.763208892719514
1v1 Average Entropy: 0.715561347065043
5v5 Average Entropy: 0.653542046507067
\> 2 team Entropy: 0.504132947296476
Dissonant 5v5 Entropy: 0.679789391820379
Dissonant 1v1 Entropy: 0.607416781929082

## Reckoner 0.04.02
- Hopefully fixed major bug with ratings
Average entropy: 
1v1 Average Entropy: 0.72915437055311
5v5 Average Entropy: 0.634649865844899
\> 2 team Entropy: 0.499680568779459
Dissonant 5v5 Entropy: 0.661970777308727
Dissonant 1v1 Entropy: 0.60319534074904

## Reckoner 0.04.04
- Remembered to account for weights in ratings
Average entropy: 0.764845154200324
1v1 Average Entropy: 0.71826166566137
5v5 Average Entropy: 0.631181665765603
\> 2 team Entropy: 0.503070506478398
Dissonant 5v5 Entropy: 0.659205448751073
Dissonant 1v1 Entropy: 0.601047462928053

## Reckoner 0.04.05
- Changed effective challenge formula to add 400 per additional player (on average) instead of 1500
Average entropy: 0.760022994138388
1v1 Average Entropy: 0.718726733217754
5v5 Average Entropy: 0.632213402512628
\> 2 team Entropy: 0.499530087970122
Dissonant 5v5 Entropy: 0.657286894858018
Dissonant 1v1 Entropy: 0.600803205371925

## Reckoner 0.04.06
- Decay reduced from 0.03 per day to 0.02
Average entropy: 0.769241361223057
1v1 Average Entropy: 0.723376641463256
5v5 Average Entropy: 0.632447632849657
\> 2 team Entropy: 0.502459177076728
Dissonant 5v5 Entropy: 0.658011888434471
Dissonant 1v1 Entropy: 0.58184885771394

## Reckoner 0.04.07
- Changed effective challenge formula to no longer use RMS (in order to handle negative ratings more properly)
Average entropy: 0.698085771436492
1v1 Average Entropy: 0.673666410050342
5v5 Average Entropy: 0.61139621139639
\> 2 team Entropy: 0.443611097396327
Dissonant 5v5 Entropy: 0.648236980834276
Dissonant 1v1 Entropy: 0.573888161629256

## Reckoner 0.04.08
- Bandage fix for initial rating not beginning at 1500
Average entropy: 0.687944722925533
1v1 Average Entropy: 0.667107170503864
5v5 Average Entropy: 0.610142744171034
\> 2 team Entropy: 0.441398934727108
Dissonant 5v5 Entropy: 0.649567235097988
Dissonant 1v1 Entropy: 0.572392729100571

## Reckoner 0.05.01
- Removed AUP and replaced with default rating, standard dev., and Glicko-based priors for win-loss
Average entropy: 0.662049868457941
1v1 Average Entropy: 0.658310713095136
5v5 Average Entropy: 0.63745045518308
\> 2 team Entropy: 0.49197915075962
Dissonant 5v5 Entropy: 0.674073602059028
Dissonant 1v1 Entropy: 0.593753825402063

## Reckoner 0.05.02
- Fixed using standard deviation in RD calculation instead of variance
Average entropy: 0.639045410445681
1v1 Average Entropy: 0.639717986752943
5v5 Average Entropy: 0.638998022779991
\> 2 team Entropy: 0.470086963505454
Dissonant 5v5 Entropy: 0.674073602059028
Dissonant 1v1 Entropy: 0.588924509844852

## Reckoner 0.05.03
- Changed effective challenge algorithm to work better with Elo/Glicko equations.
- Added eco modifier support to effective challenge algorithm
Average entropy: 0.632002657385826
1v1 Average Entropy: 0.633476399618413
5v5 Average Entropy: 0.631699062384424
\> 2 team Entropy: 0.471458746033687
Dissonant 5v5 Entropy: 0.660829047025424
Dissonant 1v1 Entropy: 0.588942580817152

## Reckoner 0.05.02
- The effective challenge algorithm's output is now used properly by the expected win-loss function.
Average entropy: 0.554689318901669
1v1 Average Entropy: 0.556326409231978
5v5 Average Entropy: 0.578229061595864
\> 2 team Entropy: 0.459010152200976
Dissonant 5v5 Entropy: 0.625266205955053
Dissonant 1v1 Entropy: 0.386331009003604

## Reckoner 0.05.03
- The effective challenge algorithm changed to work with total team strength instead of average team strength
Average entropy: 0.633051893949291

## Reckoner 0.05.04
- Effective challenge algorithm scales back to Normal(1500, 200) once again
Average entropy: 0.551491163504181
1v1 Average entropy: 0.550127834704238
5v5 Average entropy: 0.604815673301443
\>2 team Entropy: 0.464417890861863
Dissonant's 5v5 Entropy: 0.640116934047973
Dissonant 1v1 Entropy: 0.392261208091401

## Reckoner 0.05.05
- It's actually just extended Glicko, not Reckoner!
Average entropy: 0.56464520331258
1v1 Average entropy: 0.566092084342768
5v5 Average entropy: 0.608619118318539
\>2 team Entropy: 0.473056993170673
Dissonant's 5v5 Entropy: 0.638398371302757
Dissonant 1v1 Entropy: 0.427118294614986

## Reckoner 0.05.06
- Back to Reckoner
- Strength of Glicko-based win chance prior moved from 10 observations to 1 observation
Average entropy: 0.553763708808884
1v1 Average entropy: 0.556117067866659
5v5 Average entropy: 0.599023963953773
\>2 team Entropy: 0.448116555116821
Dissonant's 5v5 Entropy: 0.640333156175872
Dissonant 1v1 Entropy: 0.375112333655721

## Reckoner 0.05.07
- Fixed eco modifier bug in effective challenge algorithm
Average entropy: 0.558227837886215
1v1 Average entropy: 0.561628438102546
5v5 Average entropy: 0.599161223124245
\>2 team Entropy: 0.446624278013074
Dissonant's 5v5 Entropy: 0.639460701811167
Dissonant 1v1 Entropy: 0.374509734650744


## Reckoner 0.05.08
- Strength of Glicko-based win chance prior reverted from 1 observation to 10 observation
Average entropy: 0.556721484142076
1v1 Average entropy: 0.556429080532981
5v5 Average entropy: 0.605330662990302
\>2 team Entropy: 0.46262494709686
Dissonant's 5v5 Entropy: 0.639347782774956
Dissonant 1v1 Entropy: 0.391323670599493

## Reckoner 0.05.09
- Rating updates now based on cross entropy instead of raw win chance
Average entropy: 0.562478532812585
1v1 Average entropy: 0.563459462092208
5v5 Average entropy: 0.608460133790155
\>2 team Entropy: 0.46252556299399
Dissonant's 5v5 Entropy: 0.639347782774956
Dissonant 1v1 Entropy: 0.410466580638292

## Reckoner 0.05.10
- Strength of Glicko-based win chance prior moved from 10 observations to 2 observations
Average entropy: 0.562393653084781
1v1 Average entropy: 0.563401986145524
5v5 Average entropy: 0.608474417877464
\>2 team Entropy: 0.462339561028738
Dissonant's 5v5 Entropy: 0.639275828152754
Dissonant 1v1 Entropy: 0.410911784236515

## Reckoner 0.05.11
- Rating updates reverted to using raw win chance instead of cross entropy
Average entropy: 0.556640999468681
1v1 Average entropy: 0.556368352869901
5v5 Average entropy: 0.605267584951858
\>2 team Entropy: 0.462532712616887
Dissonant's 5v5 Entropy: 0.638876518316026
Dissonant 1v1 Entropy: 0.39115684499099

## Reckoner 0.05.12
- Teams are now summed with base 10^(1/3) instead of 10^(1/4)
Average entropy: 0.556214164239857
1v1 Average entropy: 0.556257136317549
5v5 Average entropy: 0.60409868943414
\>2 team Entropy: 0.462466305568765
Dissonant's 5v5 Entropy: 0.63866177578311
Dissonant 1v1 Entropy: 0.3912039823468

## Reckoner 0.05.12 2020-09-16 check-in
- No changes yet, just comparing value stability since last check (approximately 2020-08-24)
Average entropy: 0.556737902908173
1v1 Average entropy: 0.557494871619504
5v5 Average entropy: 0.602417379141053
\>2 team Entropy: 0.46332429751334
Dissonant's 5v5 Entropy: 0.638381038263439
Dissonant 1v1 Entropy: 0.390880709137207

Average Entropy by time period:
2020-09-01 - 2020-09-16: 0.566454308587333
2020-08-01 - 2020-09-01: 0.537610776683424
2020-07-01 - 2020-08-01: 0.531991855773833
2020-06-01 - 2020-07-01: 0.546418365238954
2020-05-01 - 2020-06-01: 0.55886397392024

## Reckoner 0.6.1 2020-10-17 (retroactive entry 2020-11-29)
- Changed rating summation formulas to be independent of game order

## Reckoner 0.7.1 2020-11-29
- Implemented iterative algorithm to approximate rating at which a player has 50% win-loss

## Reckoner 0