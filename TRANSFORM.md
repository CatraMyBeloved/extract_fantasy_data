# Source → Star Schema Transform

Mapping from the source PostgreSQL DB (`public` schema, dumped to `public/`) to the analytical star schema defined in `SCHEMA.md`.

## Source Hierarchy (confirmed from dump)

```
tournament           (region, name, tier, start_date, end_date)
  └── tournament_phase   (type: bracket/regular_season/groups/other, name, start_date, end_date)
        └── match            (team1_id, team2_id, winning_team_id, start_date)
              └── match_map      (map_id, team1_score, team2_score, winning_team_id, complete)
                    └── player_map_stats   (person_id, team_id, role, stats, cached_fantasy_score, match_start_date)
```

Supporting tables: `person`, `team`, `team_membership`, `game_map`, `game_hero`.

---

## Fact Table — `fact_player_map_stats`

One source row in `player_map_stats` → one fact row.

| Target column    | Source                                                                    | Notes                                      |
|------------------|---------------------------------------------------------------------------|--------------------------------------------|
| fk_player        | `player_map_stats.person_id`                                              | remap to dim_player surrogate id           |
| fk_team          | `player_map_stats.team_id`                                                | remap to dim_team surrogate id             |
| fk_opponent      | `match.team1_id` / `match.team2_id` (the one that isn't `team_id`)        | join via `match_map.match_id → match`      |
| fk_map           | `match_map.map_id`                                                        | remap to dim_gamemap surrogate id          |
| fk_time          | derived — see "Week assignment" below                                     |                                            |
| fk_region        | `tournament.region` → remap via dim_region                                | via `match → tournament_phase → tournament`|
| fk_team_ban      | ban picked by player's team — see "Ban alignment" below                   | remap to dim_hero surrogate id             |
| fk_opponent_ban  | ban picked by opposing team — see "Ban alignment" below                   | remap to dim_hero surrogate id             |
| role             | `player_map_stats.role`                                                   | degenerate dim: TANK / DAMAGE / SUPPORT    |
| match_id         | `match_map.match_id`                                                      | degenerate dim                             |
| fantasy_score    | `player_map_stats.cached_fantasy_score`                                   | already computed upstream                  |
| eliminations     | `player_map_stats.eliminations`                                           |                                            |
| assists          | `player_map_stats.assists`                                                |                                            |
| deaths           | `player_map_stats.deaths`                                                 |                                            |
| damage           | `player_map_stats.damage_dealt`                                           |                                            |
| healing          | `player_map_stats.healing_done`                                           |                                            |
| mitigated        | `player_map_stats.damage_mitigated`                                       |                                            |
| map_win          | `match_map.winning_team_id == player_map_stats.team_id`                   |                                            |
| match_win        | `match.winning_team_id == player_map_stats.team_id`                       |                                            |

Note: `player_map_stats.match_start_date` is denormalized on the row — useful for week assignment without joining up.

### Ban alignment

`match_map` stores bans as `team1_ban_id` / `team2_ban_id`, keyed to `match.team1_id` / `match.team2_id`. The fact row is player-centric, so remap:

```
if player_map_stats.team_id == match.team1_id:
    fk_team_ban     = match_map.team1_ban_id
    fk_opponent_ban = match_map.team2_ban_id
else:
    fk_team_ban     = match_map.team2_ban_id
    fk_opponent_ban = match_map.team1_ban_id
```

Same trick as `fk_opponent`. Both ban fields are nullable on source (not every map has recorded bans).

---

## Dimension Tables

### dim_player
Source: `person`

| Target      | Source          | Notes                                 |
|-------------|-----------------|---------------------------------------|
| id          | surrogate       |                                       |
| player_name | `person.alias`  | ingame nickname                       |
| (pronouns)  | —               | not in source, drop from schema       |

Filter: `person.job = 'player'` (others are staff/casters — check `enum_person_job`).

### dim_team
Source: `team`

| Target    | Source         | Notes                           |
|-----------|----------------|---------------------------------|
| id        | surrogate      |                                 |
| team_name | `team.name`    |                                 |

`team.initials` and `team.region` are available too if ever useful.

### dim_gamemap
Source: `game_map`

| Target   | Source            | Notes                                                   |
|----------|-------------------|---------------------------------------------------------|
| id       | surrogate         |                                                         |
| map_name | `game_map.name`   |                                                         |
| map_type | `game_map.mode`   | enum: control, escort, flashpoint, hybrid, push, clash  |

### dim_hero
Source: `game_hero`

| Target    | Source           | Notes                                    |
|-----------|------------------|------------------------------------------|
| id        | surrogate        |                                          |
| hero_name | `game_hero.name` |                                          |
| role      | `game_hero.role` | if available on source                   |

Referenced only via `fk_team_ban` / `fk_opponent_ban` on the fact. No hero-per-player-per-map pick data exists upstream (confirmed in `owtvgg` — `player_map_stats` has no `hero_id`, only `role`).

### dim_time
Derived. One row per (tournament_phase, week_number).

| Target        | Source                                    | Notes                              |
|---------------|-------------------------------------------|------------------------------------|
| id            | surrogate                                 |                                    |
| week_number   | derived from match_start_date             | see "Week assignment"              |
| stage_id      | `tournament_phase.id`                     | source phase id                    |
| stage_name    | `tournament_phase.name`                   |                                    |
| stage_type    | `tournament_phase.type`                   | regular_season / bracket / (?)     |
| tournament_id | `tournament.id`                           |                                    |
| fk_region     | via dim_region                            |                                    |

### dim_region
Derived. One row per in-scope region.

| Target      | Source                        | Notes                  |
|-------------|-------------------------------|------------------------|
| id          | surrogate                     |                        |
| region_name | `tournament.region` (filtered)| NA / EMEA / korea only |

---

## Decisions & Open Items

### 1. Hero data — resolved
No hero-per-player-per-map pick data exists in source. Confirmed by reading `owtvgg/apps/web/db/collections/tournaments/player-map-stats.ts` — only `role` is recorded per player per map.

**Decision:** keep `dim_hero` sourced from `game_hero`. Reference it via `fk_team_ban` and `fk_opponent_ban` on the fact (see "Ban alignment"). Add `role` as a degenerate dim on the fact.

### 2. Week number — resolved
No week column in source. Derive per phase using literal 7-day windows from phase start:
```
week_number = floor((match_start_date - phase.start_date) / 7 days) + 1
```
No canonical Mon–Sun calendar week in the source product (fantasy rounds are date-ranged, not weekly). `match.faceit_round` remains unclear semantically; ignore for now.

### 3. Phase types — resolved
Source enum: `bracket, regular_season, groups, other`. Data inspection (2026-04-20) showed:

| type            | name                                | count |
|-----------------|-------------------------------------|-------|
| bracket         | Playoffs                            | 3     |
| regular_season  | Regular Season                      | 3     |
| regular_season  | Last Chance Qualifier               | 1     |
| regular_season  | Playoffs Seeding Decider Matches    | 1     |
| groups          | —                                   | 0     |
| other           | —                                   | 0     |

LCQ and seeding deciders are encoded as `type='regular_season'` at the source, and the source fantasy pipeline scores them as regular season. We follow suit — no name-based exclusion.

**Filter rule:**
```sql
tp.type IN ('regular_season', 'bracket')
```

`groups` / `other` are empty today. Revisit scope if they populate in future data.

**Note:** this supersedes SCHEMA.md's line about excluding LCQ / seeding decider phases — update SCHEMA.md to match.

### 4. Region filter — decided
In scope: `NA, EMEA, korea`. Filter at tournament level: `WHERE tournament.region IN ('NA','EMEA','korea')`.

Data inspection (2026-04-20) confirmed **no `global` tournaments exist** in the source today, so the filter is currently a no-op beyond excluding `asia / china / japan / pacific`. If `global` entries appear later (e.g., OWCS Finals), revisit — may want to derive region from `team.region` instead of `tournament.region`.

### 5. Nullable `person_id` — resolved
Filter out null-person rows silently during fact-table build. Matches how `owtvgg` handles them: `recalculate-round-stats.ts:54` and `propagate-map-scores-to-rosters.ts:34` both skip rows where person is null.

Data inspection (2026-04-20) confirmed **zero null-person rows** in current data. Filter is defensive / future-proofing.

### 6. `match_map.complete` flag — configurable
`complete=true` means stats are final and counted in the app's fantasy scoring. `complete=false` rows can exist with non-zero stats and a computed `cached_fantasy_score`, but the app doesn't count them.

**Decision:** expose as a pipeline parameter `filter_complete_only: bool`, default **False** for now (current dataset is curated manually, all maps are considered entered correctly). Switch to `True` once we move to a recurring ingest where partial/in-progress maps will exist in the source.

Data inspection (2026-04-20) confirmed **no incomplete maps** in current data.

### 7. `cached_fantasy_score` — resolved
Formula found at `owtvgg/apps/web/db/collections/fantasy/hooks/compute-fantasy-score.ts:4-27`:
```
score = floor(eliminations / 3)
      + (deaths * -1)
      + floor((damage_dealt + healing_done) / 1000) * 0.5
```
Computed in a `beforeChange` hook on `player_map_stats`, so it's always recomputed on any stat update — effectively never stale. Inputs: **only** `eliminations`, `deaths`, `damage_dealt`, `healing_done`. `assists`, `damage_mitigated`, and `role` are **not** used in scoring (keep them in the fact for independent analysis). Null is theoretically possible only if a row was inserted bypassing the hook; treat as rare.

---

## Pipeline Order

1. Load dims from source (person, team, game_map, game_hero, tournament + tournament_phase)
2. Build `dim_region` from distinct in-scope regions
3. Build `dim_time` from distinct (tournament_phase, week) pairs — requires joining up from match_start_date
4. Assign surrogate ids for all dims
5. Load `player_map_stats` + joins → `fact_player_map_stats`, remapping natural ids to dim surrogate ids; apply ban alignment to produce `fk_team_ban` / `fk_opponent_ban`
6. Filter to in-scope regions + phase types + non-null person_id (+ `complete=true` if `filter_complete_only` is enabled)
7. Write to parquet
8. Downstream: derive `fact_team_match_stats` aggregate
