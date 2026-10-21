"""Cliente de la API DB Stations (DB API Marketplace).

Resuelve el listado de estaciones Fernverkehr (nombre + EVA) que se usara para
enriquecer la telemetria nacional con los numeros de tren reales de la API
Timetables. El catalogo se cachea en ``data/db_stations.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

import requests

from config import DB_API_KEY, DB_CLIENT_ID

DB_STATIONS_BASE = "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2"
CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "db_stations.json"
REQUEST_TIMEOUT = 30


def _headers() -> dict[str, str]:
    return {
        "DB-Client-Id": DB_CLIENT_ID,
        "DB-Api-Key": DB_API_KEY,
        "Accept": "application/json",
    }


def fetch_stations(force: bool = False) -> list[dict]:
    """Listado de estaciones (id=EVA, name, category) del API de DB.

    El resultado se cachea en disco para no repetir la llamada (el catalogo
    nacional completo son ~500 KB).
    """
    if CACHE_PATH.exists() and not force:
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass

    stations: list[dict] = []
    offset = 0
    page_size = 100
    while True:
        response = requests.get(
            f"{DB_STATIONS_BASE}/stations",
            headers=_headers(),
            params={"limit": page_size, "offset": offset},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        page = payload.get("result", [])
        stations.extend(page)
        total = payload.get("total", len(stations))
        offset += page_size
        if not page or offset >= total:
            break

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(stations, ensure_ascii=False), encoding="utf-8"
    )
    return stations


def normalize_name(name: str) -> str:
    """Normaliza un nombre de estacion para casar los nombres de gtfs.de con
    los de la API de DB (espacios, comas, sufijos y abreviaturas).

    gtfs.de y DB escriben los nombres de forma distinta: gtfs usa
    'Frankfurt(Main)Hbf' y 'Berlin Hbf' mientras DB usa 'Frankfurt (Main) Hbf'
    y 'Berlin Hauptbahnhof'. Se unifican abreviaturas (Hbf/Hauptbahnhof,
    Flughafen/Fernbf...) para que casen.
    """
    text = (
        name.lower()
        .replace("(", " ")
        .replace(")", " ")
        .replace(",", " ")
        .replace("-", " ")
        .replace(".", " ")
    )
    text = " ".join(text.split())
    # Abreviaturas equivalentes.
    replacements = {
        "hbf": "hauptbahnhof",
        "hbf ": "hauptbahnhof ",
        " flughafen": " flughafen",
        "fernbf": "fernbahnhof",
        "s dkreuz": "suedkreuz",
        "suedkreuz": "suedkreuz",
    }
    for short, full in replacements.items():
        text = text.replace(short, full)
    return text.strip()


def _primary_eva(station: dict) -> str:
    """Devuelve el EVA principal de una estacion del API de DB."""
    evas = station.get("evaNumbers") or []
    for eva in evas:
        if eva.get("isMain") and eva.get("number"):
            return str(eva["number"])
    if evas and evas[0].get("number"):
        return str(evas[0]["number"])
    return ""


def build_name_index(stations: list[dict]) -> dict[str, str]:
    """Mapa nombre-normalizado -> EVA (nos quedamos con la mejor categoria).

    Se indexan tanto el nombre oficial como las variantes habituales que usa
    gtfs.de (p. ej. 'Frankfurt(Main)Hbf' vs 'Frankfurt(Main) Hbf').
    """
    index: dict[str, str] = {}
    best_category: dict[str, int] = {}
    for station in stations:
        eva = _primary_eva(station)
        name = station.get("name", "")
        if not eva or not name:
            continue
        key = normalize_name(name)
        try:
            category = int(station.get("category", 0) or 0)
        except (TypeError, ValueError):
            category = 0
        if key not in index or category > best_category.get(key, 0):
            index[key] = eva
            best_category[key] = category
    return index
