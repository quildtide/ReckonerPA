# A Post Mortem for the Reckoner Rating System v\<1 (2019-2021)

## The Motivations Behind Reckoner

At some point in early 2019, I decided that I wanted to work on a rating system for a video game that I play, Planetary Annihilation.

Planetary Annihilation is a Real-Time Strategy (RTS) game with a relatively small player population, with an average of around 200 players online at any point (although the amount of players actively playing a multiplayer match at any point is likely far lower). This number wasn't necessarily always the case though, and the playerbase was once larger in the past.

Planetary Annihilation has a ranked system for 1v1 matches, where two players are paired against each other after entering a competitive matchmaking queue. On the other hand, many of the matches played in Planetary Annihilation are not arranged through this competitive matchmaking queue, but are instead "pick-up games" (PUGs) with the following characteristics:

- The exact rules of the game may be slightly (or massively, in some cases!) altered in ways that players can be aware of—although players might sometimes not notice relevant rule changes.
- The amount of teams per match may differ from match to match
- The amount of players per team may differ from match to match, and sometimes, teams might even be uneven in size
- Players are aware of who is on their team and who is on an opposing team before the match starts
- Players are theoretically able to leave a lobby before the match starts if they do not like the conditions of the lobby
- There are no technical barriers to players of vastly different skill levels encountering each other in a lobby
- Individual players can sometimes be playing under altered conditions compared to others in the same lobby, like with a handicap or different loss conditions.

In addition, in Planetary Annihilation, there are some other interesting environmental factors at play:

- Most of the more experienced players know each other due to the smaller playerbase size
- Players can change their names easily, which can sometimes cause an experienced player to evade detection
- Players can also create new accounts by buying a new copy of Planetary Annihilation, which makes tracking them even more difficult
- I have personally witnessed strong players changing their names in order to negotiate better terms for themselves in a lobby than what they'd receive if they were recognized.

For these reasons, I endeavoured to mod in a rating system for lobbies, but I also desired to create a custom system to deal with 