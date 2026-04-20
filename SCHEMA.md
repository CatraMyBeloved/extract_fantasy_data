# Analytical Schema Design

## Scope
- NA, EMEA, and Korea regional play (regular season + playoffs)
- LAN events excluded for now
- LCQ and seeding decider phases are included — source encodes them as `type='regular_season'` and the source fantasy pipeline scores them as regular season, so we follow suit
- Korea weeks are NOT synced with NA/EMEA — never compare week numbers across regions
- Fantasy has two roster pools: Korea and non-Korea (western) — frontend should mirror this split

## Source Hierarchy
```
tournament (has region)
  └── tournament_phase (regular_season / bracket)
        └── match
              └── match_map
                    └── player_map_stats
```

## Time Hierarchy
```
region (NA / EMEA)
  └── tournament / stage
        └── week (regular weeks + playoff bracket treated as a week)
              └── match
                    └── map
```

---

## Fact Table — `fact_player_map_stats`
Grain: one row per player per map played.

| Column | Type | Notes |
|---|---|---|
| fk_player | int | → dim_player |
| fk_team | int | → dim_team (player's team) |
| fk_opponent | int | → dim_team (opposing team, role-playing dim) |
| fk_map | int | → dim_gamemap |
| fk_time | int | → dim_time |
| fk_region | int | → dim_region |
| fk_team_ban | int | → dim_hero — ban picked by player's team (nullable) |
| fk_opponent_ban | int | → dim_hero — ban picked by opposing team (nullable) |
| role | enum | TANK / DAMAGE / SUPPORT — degenerate dim |
| match_id | int | degenerate dim, for grouping maps per match |
| fantasy_score | float | pre-computed in source DB |
| eliminations | int | |
| assists | int | not used in fantasy score, kept for analysis |
| deaths | int | |
| damage | int | |
| healing | int | |
| mitigated | int | not used in fantasy score, kept for analysis |
| map_win | bool | did player's team win this map |
| match_win | bool | did player's team win the match |

---

## Dimension Tables

### dim_player

| Column      | Type | Notes                             |
|-------------|------|-----------------------------------|
| id          | int  | id to fk to fact table            |
| player_name | str  | Player nickname ingame            |
| pronouns    | str  | potentially include? idk prob not |


### dim_team

| Column    | Type | Notes                  |
|-----------|------|------------------------|
| id        | int  | id to fk to fact table |
| team_name | str  | team name              |

- Both fk_team and fk_opponent on the fact point here (role-playing dimension)
- Team membership history is captured naturally at fact level — no SCD needed

### dim_gamemap

| Column   | Type | Notes                  |
|----------|------|------------------------|
| id       | int  | id to fk to fact table |
| map_name | str  | map name               |
| map_type | enum | gamemode               |

- Map implies mode, so mode lives here rather than on the fact

### dim_hero

| Column | Type | Notes                  |
|--------|------|------------------------|
| id     | int  | id to fk to fact table |
| name   | str  | hero name              |
| role   | str  | hero role              |

- Referenced only via `fk_team_ban` / `fk_opponent_ban` on the fact — source does not record which hero each player played per map, only bans per match_map


### dim_time
Grain: one row per week. Flat dim holding the full hierarchy (week → stage → tournament → region) so one join gives you everything.

| Column        | Type | Notes                                |
|---------------|------|--------------------------------------|
| id            | int  | id to fk to fact table               |
| week_number   | int  | week number within stage             |
| stage_id      | int  | source stage/phase id                |
| stage_name    | str  | e.g. "Regular Season", "Playoffs"    |
| stage_type    | enum | regular_season / bracket             |
| tournament_id | int  | source tournament id                 |
| fk_region     | int  | → dim_region                         |

### dim_region
Exists for schema clarity — only a few rows, but nobody would guess region lives inside dim_time.

| Column      | Type | Notes                  |
|-------------|------|------------------------|
| id          | int  | id to fk to fact table |
| region_name | str  | NA / EMEA / Korea      |

- fk_region is a direct FK on the fact (common filter, avoids extra join through dim_time)

---

## Aggregate Fact Tables (derived, built downstream)

### fact_team_match_stats
- Grain: one row per team per match
- Derived from fact_player_map_stats
- Purpose: team strength metrics, win/loss record, strength of schedule

---

## Separate Pipeline — Player Price

### fact_player_price
- Grain: one row per player per week
- Joins to dim_player and dim_time
- Source TBD

---

## Analytical Questions to Answer
- How has a player performed in recent maps / matches / weeks?
- Is their performance trending up or down?
- How do they perform vs strong teams vs weak teams?
- How do they perform in wins vs losses?
- How do they perform on specific maps?
- Stats per fantasy price — value rating
