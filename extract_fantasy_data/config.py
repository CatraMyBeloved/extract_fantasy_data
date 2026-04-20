import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


REGIONS_IN_SCOPE: tuple[str, ...] = ("NA", "EMEA", "korea")
PHASE_TYPES_IN_SCOPE: tuple[str, ...] = ("regular_season", "bracket")

REGION_SURROGATES: dict[str, int] = {"NA": 1, "EMEA": 2, "korea": 3}

# Reference only: fantasy score formula lives in owtvgg
# (apps/web/db/collections/fantasy/hooks/compute-fantasy-score.ts). We pass the
# pre-computed cached_fantasy_score straight through — these weights are kept
# here solely for future sanity-check code.
FANTASY_WEIGHTS = {
    "elims_divisor": 3,
    "death": -1,
    "heal_and_dmg_per_1000": 0.5,
}


@dataclass(frozen=True)
class Config:
    database_url: str
    output_dir: Path
    regions: tuple[str, ...]
    filter_complete_only: bool
    dry_run: bool


def load_config(
    output_dir: Path,
    regions: tuple[str, ...],
    filter_complete_only: bool,
    dry_run: bool,
) -> Config:
    load_dotenv()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL not set (expected in .env or environment)")
    return Config(
        database_url=database_url,
        output_dir=output_dir,
        regions=regions,
        filter_complete_only=filter_complete_only,
        dry_run=dry_run,
    )
