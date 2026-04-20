"""ETL: owtvgg Postgres source → parquet star-schema warehouse.

See TRANSFORM.md and SCHEMA.md for target schema.
"""
import argparse
import logging
import sys
from pathlib import Path

from extract_fantasy_data import extract, load, transform
from extract_fantasy_data.config import REGIONS_IN_SCOPE, load_config
from extract_fantasy_data.db import connect
from extract_fantasy_data.logging_setup import setup_logging

log = logging.getLogger("main")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./output"),
        help="Output directory (default: ./output)",
    )
    p.add_argument(
        "--filter-complete-only",
        action="store_true",
        help="Drop player_map_stats rows whose match_map.complete != true",
    )
    p.add_argument(
        "--regions",
        default=",".join(REGIONS_IN_SCOPE),
        help="Comma-separated region whitelist (default: NA,EMEA,korea)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the pipeline but write no files",
    )
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")
    return p.parse_args()


def run() -> int:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    regions = tuple(r.strip() for r in args.regions.split(",") if r.strip())
    cfg = load_config(
        output_dir=args.output_dir,
        regions=regions,
        filter_complete_only=args.filter_complete_only,
        dry_run=args.dry_run,
    )

    log.info("=== ETL start ===")
    log.info(
        "config: regions=%s, filter_complete_only=%s, output_dir=%s, dry_run=%s",
        cfg.regions,
        cfg.filter_complete_only,
        cfg.output_dir,
        cfg.dry_run,
    )

    with connect(cfg.database_url) as conn:
        log.info("--- extract ---")
        tournament = extract.fetch_tournament(conn)
        phase = extract.fetch_tournament_phase(conn)
        match = extract.fetch_match(conn)
        match_map = extract.fetch_match_map(conn)
        pms = extract.fetch_player_map_stats(conn)
        person = extract.fetch_person(conn)
        team = extract.fetch_team(conn)
        game_map = extract.fetch_game_map(conn)
        game_hero = extract.fetch_game_hero(conn)

    log.info("--- build dims ---")
    dim_region = transform.build_dim_region()
    dim_player = transform.build_dim_player(person)
    dim_team = transform.build_dim_team(team)
    dim_gamemap = transform.build_dim_gamemap(game_map)
    dim_hero = transform.build_dim_hero(game_hero)

    log.info("--- build fact ---")
    intermediate = transform.build_intermediate_fact(
        pms=pms, match_map=match_map, match=match, phase=phase, tournament=tournament
    )
    filtered = transform.apply_fact_filters(
        intermediate,
        regions=cfg.regions,
        filter_complete_only=cfg.filter_complete_only,
    )
    dim_time = transform.build_dim_time(filtered, dim_region)
    fact = transform.finalize_fact(filtered, dim_time)

    log.info("--- write ---")
    load.clear_output(cfg.output_dir, dry_run=cfg.dry_run)
    load.write_dim(dim_region, cfg.output_dir, "dim_region", cfg.dry_run)
    load.write_dim(dim_player, cfg.output_dir, "dim_player", cfg.dry_run)
    load.write_dim(dim_team, cfg.output_dir, "dim_team", cfg.dry_run)
    load.write_dim(dim_gamemap, cfg.output_dir, "dim_gamemap", cfg.dry_run)
    load.write_dim(dim_hero, cfg.output_dir, "dim_hero", cfg.dry_run)
    load.write_dim(dim_time, cfg.output_dir, "dim_time", cfg.dry_run)
    load.write_fact(fact, cfg.output_dir, cfg.dry_run)

    log.info("=== summary ===")
    log.info("dim_region:  %d rows", dim_region.height)
    log.info("dim_player:  %d rows", dim_player.height)
    log.info("dim_team:    %d rows", dim_team.height)
    log.info("dim_gamemap: %d rows", dim_gamemap.height)
    log.info("dim_hero:    %d rows", dim_hero.height)
    log.info("dim_time:    %d rows", dim_time.height)
    log.info("fact:        %d rows", fact.height)
    log.info("=== ETL done ===")
    return 0


if __name__ == "__main__":
    sys.exit(run())
