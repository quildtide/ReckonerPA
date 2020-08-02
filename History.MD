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