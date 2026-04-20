"""Transform source DataFrames into dim/fact tables.

See TRANSFORM.md for the authoritative mapping.
"""
import logging

import polars as pl

from .config import PHASE_TYPES_IN_SCOPE, REGION_SURROGATES

log = logging.getLogger(__name__)


def build_dim_region() -> pl.DataFrame:
    df = pl.DataFrame(
        {
            "id": list(REGION_SURROGATES.values()),
            "region_name": list(REGION_SURROGATES.keys()),
        }
    ).with_columns(pl.col("id").cast(pl.Int64))
    log.info("built dim_region: %d rows", df.height)
    return df


def build_dim_player(person: pl.DataFrame) -> pl.DataFrame:
    before = person.height
    df = person.filter(pl.col("job") == "player").select(
        pl.col("id"),
        pl.col("alias").alias("player_name"),
    )
    log.info(
        "built dim_player: %d rows (dropped %d non-players)",
        df.height,
        before - df.height,
    )
    return df


def build_dim_team(team: pl.DataFrame) -> pl.DataFrame:
    df = team.select(pl.col("id"), pl.col("name").alias("team_name"))
    log.info("built dim_team: %d rows", df.height)
    return df


def build_dim_gamemap(game_map: pl.DataFrame) -> pl.DataFrame:
    df = game_map.select(
        pl.col("id"),
        pl.col("name").alias("map_name"),
        pl.col("mode").alias("map_type"),
    )
    log.info("built dim_gamemap: %d rows", df.height)
    return df


def build_dim_hero(game_hero: pl.DataFrame) -> pl.DataFrame:
    df = game_hero.select(
        pl.col("id"),
        pl.col("name").alias("hero_name"),
        pl.col("game_role").alias("role"),
    )
    log.info("built dim_hero: %d rows", df.height)
    return df


def build_intermediate_fact(
    pms: pl.DataFrame,
    match_map: pl.DataFrame,
    match: pl.DataFrame,
    phase: pl.DataFrame,
    tournament: pl.DataFrame,
) -> pl.DataFrame:
    """Joined + derived columns (ban alignment, opponent, wins, week) — no fk_time yet."""
    mm = match_map.select(
        pl.col("id").alias("match_map_id"),
        pl.col("match_id"),
        pl.col("map_id"),
        pl.col("team1_ban_id"),
        pl.col("team2_ban_id"),
        pl.col("winning_team_id").alias("mm_winning_team_id"),
        pl.col("complete"),
    )
    m = match.select(
        pl.col("id").alias("match_id"),
        pl.col("tournament_phase_id"),
        pl.col("team1_id").alias("m_team1_id"),
        pl.col("team2_id").alias("m_team2_id"),
        pl.col("winning_team_id").alias("m_winning_team_id"),
        pl.col("start_date").alias("m_start_date"),
    )
    tp = phase.select(
        pl.col("id").alias("tournament_phase_id"),
        pl.col("tournament_id"),
        pl.col("type").alias("phase_type"),
        pl.col("name").alias("phase_name"),
        pl.col("start_date").alias("phase_start_date"),
    )
    t = tournament.select(
        pl.col("id").alias("tournament_id"),
        pl.col("region"),
    )

    fact = (
        pms.join(mm, on="match_map_id", how="left")
        .join(m, on="match_id", how="left")
        .join(tp, on="tournament_phase_id", how="left")
        .join(t, on="tournament_id", how="left")
    )

    is_team1 = pl.col("team_id") == pl.col("m_team1_id")
    match_start = pl.col("match_start_date").fill_null(pl.col("m_start_date"))

    fact = fact.with_columns(
        [
            ((match_start - pl.col("phase_start_date")).dt.total_days() // 7 + 1)
            .cast(pl.Int64)
            .alias("week_number"),
            pl.when(is_team1)
            .then(pl.col("team1_ban_id"))
            .otherwise(pl.col("team2_ban_id"))
            .alias("fk_team_ban"),
            pl.when(is_team1)
            .then(pl.col("team2_ban_id"))
            .otherwise(pl.col("team1_ban_id"))
            .alias("fk_opponent_ban"),
            pl.when(is_team1)
            .then(pl.col("m_team2_id"))
            .otherwise(pl.col("m_team1_id"))
            .alias("fk_opponent"),
            (pl.col("mm_winning_team_id") == pl.col("team_id")).alias("map_win"),
            (pl.col("m_winning_team_id") == pl.col("team_id")).alias("match_win"),
            pl.col("region")
            .replace_strict(REGION_SURROGATES, default=None, return_dtype=pl.Int64)
            .alias("fk_region"),
        ]
    )

    log.info("built intermediate fact: %d rows", fact.height)
    return fact


def apply_fact_filters(
    fact: pl.DataFrame,
    regions: tuple[str, ...],
    filter_complete_only: bool,
) -> pl.DataFrame:
    steps: list[tuple[str, pl.Expr]] = [
        ("region not in scope", pl.col("region").is_in(list(regions))),
        ("phase type not in scope", pl.col("phase_type").is_in(list(PHASE_TYPES_IN_SCOPE))),
        ("null person_id", pl.col("person_id").is_not_null()),
    ]
    if filter_complete_only:
        steps.append(("incomplete map", pl.col("complete") == True))  # noqa: E712

    for label, predicate in steps:
        before = fact.height
        fact = fact.filter(predicate)
        dropped = before - fact.height
        if dropped:
            log.info("dropped %d rows: %s", dropped, label)
        else:
            log.debug("no rows dropped: %s", label)

    log.info("fact after filters: %d rows", fact.height)
    return fact


def build_dim_time(
    filtered_fact: pl.DataFrame, dim_region: pl.DataFrame
) -> pl.DataFrame:
    """Distinct (phase, week) pairs present in the filtered fact."""
    null_week = filtered_fact.filter(pl.col("week_number").is_null()).height
    if null_week:
        log.warning(
            "dropping %d fact rows from dim_time derivation (null week_number — likely null phase.start_date)",
            null_week,
        )

    region_lookup = dim_region.select(
        pl.col("id").alias("fk_region"),
        pl.col("region_name"),
    )

    df = (
        filtered_fact.select(
            pl.col("tournament_phase_id").alias("stage_id"),
            pl.col("phase_name").alias("stage_name"),
            pl.col("phase_type").alias("stage_type"),
            pl.col("tournament_id"),
            pl.col("region").alias("region_name"),
            pl.col("week_number"),
        )
        .filter(pl.col("week_number").is_not_null())
        .unique()
        .join(region_lookup, on="region_name", how="left")
        .drop("region_name")
        .sort("stage_id", "week_number")
        .with_row_index("id", offset=1)
        .with_columns(pl.col("id").cast(pl.Int64))
        .select(
            "id",
            "week_number",
            "stage_id",
            "stage_name",
            "stage_type",
            "tournament_id",
            "fk_region",
        )
    )
    log.info("built dim_time: %d rows", df.height)
    return df


def finalize_fact(
    filtered_fact: pl.DataFrame, dim_time: pl.DataFrame
) -> pl.DataFrame:
    """Join fk_time and project to final fact schema.

    Keeps `region` and `tournament_id` columns on the output for hive-style
    partitioning at write time; `load.py` strips them out of the file data.
    """
    time_lookup = dim_time.select(
        pl.col("id").alias("fk_time"),
        pl.col("stage_id").alias("tournament_phase_id"),
        pl.col("week_number"),
    )
    fact = filtered_fact.join(
        time_lookup, on=["tournament_phase_id", "week_number"], how="left"
    )

    fact = fact.select(
        pl.col("person_id").alias("fk_player"),
        pl.col("team_id").alias("fk_team"),
        pl.col("fk_opponent"),
        pl.col("map_id").alias("fk_map"),
        pl.col("fk_time"),
        pl.col("fk_region"),
        pl.col("fk_team_ban"),
        pl.col("fk_opponent_ban"),
        pl.col("role"),
        pl.col("match_id"),
        pl.col("cached_fantasy_score").cast(pl.Float64).alias("fantasy_score"),
        pl.col("eliminations").cast(pl.Int64),
        pl.col("assists").cast(pl.Int64),
        pl.col("deaths").cast(pl.Int64),
        pl.col("damage_dealt").cast(pl.Int64).alias("damage"),
        pl.col("healing_done").cast(pl.Int64).alias("healing"),
        pl.col("damage_mitigated").cast(pl.Int64).alias("mitigated"),
        pl.col("map_win"),
        pl.col("match_win"),
        pl.col("region"),
        pl.col("tournament_id"),
    )
    log.info("finalized fact: %d rows", fact.height)
    return fact
