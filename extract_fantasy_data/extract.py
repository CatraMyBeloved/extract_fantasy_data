import logging

import polars as pl
import psycopg

from .db import fetch_df

log = logging.getLogger(__name__)


def _fetch(conn: psycopg.Connection, table: str, sql: str) -> pl.DataFrame:
    df = fetch_df(conn, sql)
    log.info("extracted %d rows from %s", df.height, table)
    return df


def fetch_tournament(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "tournament",
        "SELECT id, name, region, tier, start_date, end_date FROM tournament",
    )


def fetch_tournament_phase(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "tournament_phase",
        """
        SELECT id, tournament_id, type, name, start_date, end_date
        FROM tournament_phase
        """,
    )


def fetch_match(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "match",
        """
        SELECT id, tournament_phase_id, team1_id, team2_id, winning_team_id,
               start_date
        FROM match
        """,
    )


def fetch_match_map(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "match_map",
        """
        SELECT id, match_id, map_id, team1_ban_id, team2_ban_id,
               winning_team_id, complete
        FROM match_map
        """,
    )


def fetch_player_map_stats(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "player_map_stats",
        """
        SELECT id, match_map_id, person_id, team_id, role,
               eliminations, assists, deaths,
               damage_dealt, healing_done, damage_mitigated,
               cached_fantasy_score, match_start_date
        FROM player_map_stats
        """,
    )


def fetch_person(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "person",
        "SELECT id, alias, job FROM person",
    )


def fetch_team(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "team",
        "SELECT id, name FROM team",
    )


def fetch_game_map(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "game_map",
        "SELECT id, name, mode FROM game_map",
    )


def fetch_game_hero(conn: psycopg.Connection) -> pl.DataFrame:
    return _fetch(
        conn,
        "game_hero",
        "SELECT id, name, game_role FROM game_hero",
    )
