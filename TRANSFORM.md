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
| fk_hero          | **NOT AVAILABLE** — see Issue 1                                           |                                            |
| fk_region        | `tournament.region` → remap via dim_region                                | via `match → tournament_phase → tournament`|
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
See Issue 1 — source data does not record which hero each player played. This dim may need to be dropped.

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

## Open Issues

### 1. No hero per player per map — `dim_hero` / `fk_hero` not fillable
`player_map_stats` has no `hero_id` column. The only hero references are `match_map.team1_ban_id` and `team2_ban_id` (bans, not picks). `game_hero` exists but isn't linked to performance data.

`player_map_stats.role` (enum: TANK / DAMAGE / SUPPORT) is the only hero-related signal.

**Options:**
- (A) Drop `fk_hero` and `dim_hero`. Add `role` as a degenerate dim on the fact.
- (B) Keep `dim_hero` but repurpose for ban analysis only, tied to match_map not to fact_player_map_stats.
- (C) Confirm there really is no hero-per-map data anywhere (check for views, or ask whoever owns the source DB).

Recommendation: **(A)** unless hero picks are going to be added upstream. Role is still useful analytically ("how do damage players perform on this map").

### 2. No week number in source — must be derived
Neither `tournament_phase` nor `match` have a week column. Nearest signals:
- `tournament_phase.start_date` / `end_date`
- `match.start_date` / `player_map_stats.match_start_date`
- `match.faceit_round` (unclear semantics — could be useful)

**Suggested derivation:**
```
week_number = floor((match_start_date - phase.start_date) / 7 days) + 1
```
Scoped per phase. Bracket phases collapse to a single "week" if they span ≤ 7 days, else split.

**Open question:** does the fantasy product have a canonical week boundary (e.g., Monday-to-Sunday)? If so, use that instead of relative-to-phase-start.

### 3. Phase types — which are in scope?
Source enum: `bracket, regular_season, groups, other`.
SCHEMA.md scope: regular_season + bracket only.

**Decisions needed:**
- `groups` — group stages before bracket. Likely in scope if tournaments use them?
- `other` — catch-all. Probably exclude.

Also confirm: LCQ and seeding decider phases are excluded. Are these encoded as `other`, or stored with specific `name` values inside `regular_season` / `bracket`? Need to inspect data.

### 4. Region filter
Source enum: `asia, EMEA, NA, china, japan, korea, pacific, global`.
In scope: NA, EMEA, korea.

Filter applied at the tournament level: `WHERE tournament.region IN ('NA', 'EMEA', 'korea')`.

Confirm: are there tournaments tagged `global` that contain NA/EMEA/korea matches we'd want (e.g., LANs)? SCHEMA.md says LAN excluded, so probably safe to hard-filter.

### 5. `player_map_stats.person_id` is nullable
Source has `person_id integer` (no `not null`). Some rows may not resolve to a person — probably should be filtered out of the fact table, but worth checking how common this is.

### 6. `match_map.complete` flag
Boolean, nullable. Likely want to filter to `complete = true` so partial/cancelled maps don't pollute stats. Confirm.

### 7. Fantasy score `cached_` prefix
`cached_fantasy_score` implies it's a derived/cached value upstream. Need to understand:
- When is the cache populated? (affects freshness)
- Can it be null or stale?
- What's the underlying formula? (for sanity checks / extending)

---

## Pipeline Order

1. Load dims from source (person, team, game_map, tournament + tournament_phase)
2. Build `dim_region` from distinct in-scope regions
3. Build `dim_time` from distinct (tournament_phase, week) pairs — requires joining up from match_start_date
4. Assign surrogate ids for all dims
5. Load `player_map_stats` + joins → `fact_player_map_stats`, remapping natural ids to dim surrogate ids
6. Filter to in-scope regions + phase types + complete maps + non-null person_id
7. Write to parquet
8. Downstream: derive `fact_team_match_stats` aggregate
