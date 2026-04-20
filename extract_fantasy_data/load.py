"""Write dims and fact to parquet.

Dims are flat files. Fact is hive-partitioned by region / tournament_id.
"""
import logging
import shutil
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)


def clear_output(output_dir: Path, dry_run: bool) -> None:
    if dry_run:
        log.info("[dry-run] would clear %s", output_dir)
        return
    if output_dir.exists():
        log.info("clearing existing output at %s", output_dir)
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def write_dim(df: pl.DataFrame, output_dir: Path, name: str, dry_run: bool) -> None:
    path = output_dir / f"{name}.parquet"
    if dry_run:
        log.info("[dry-run] would write %s (%d rows)", path, df.height)
        return
    df.write_parquet(path)
    log.info("wrote %s (%d rows)", path, df.height)


def write_fact(fact: pl.DataFrame, output_dir: Path, dry_run: bool) -> None:
    path = output_dir / "fact_player_map_stats"
    if dry_run:
        partitions = fact.select("region", "tournament_id").unique().height
        log.info(
            "[dry-run] would write %s partitioned by region/tournament_id (%d rows, %d partitions)",
            path,
            fact.height,
            partitions,
        )
        return
    fact.write_parquet(path, partition_by=["region", "tournament_id"])
    log.info(
        "wrote %s partitioned by region/tournament_id (%d rows)", path, fact.height
    )
