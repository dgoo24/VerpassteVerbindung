"""Enriquecimiento con feeds GTFS-DE: linea y destino de cada tren.

La API Timetables de DB no incluye ni la linea (RB45, ICE 25, RE50...) ni el
destino de un tren en una estacion. Los feeds GTFS-DE abiertos (gtfs.de,
Creative Commons 4.0) si los traen: por viaje se conoce la linea
(route_short_name) y el ``stop_headsign`` en la parada, que marca el destino
del tren.

Se usan dos feeds del paquete de descarga libre (validez 7 dias):

* ``rv`` (regional, https://download.gtfs.de/germany/rv_free/latest.zip)
  para RE / RB / IRE / HLB / CAN...
* ``fv`` (larga distancia, https://download.gtfs.de/germany/fv_free/latest.zip)
  para ICE / IC / EC / TGV / FlixTrain...

Ninguno de los dos incluye numeros de tren, asi que el emparejamiento se hace
por hora de paso por la estacion monitorizada (ventana de +-10 min). El feed
regional solo contiene trenes regionales y el de larga distancia solo trenes de
larga distancia, por lo que la categoria del tren decide que indice consultar.

El resultado se cachea en disco (data/gtfs_<feed>/) y solo se re-descarga si el
indice tiene mas de GTFS_REFRESH_DAYS dias o no existe.
"""

from __future__ import annotations

import csv
import io
import json
import os
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

GTFS_RV_URL = os.getenv(
    "GTFS_RV_URL", "https://download.gtfs.de/germany/rv_free/latest.zip"
)
GTFS_FV_URL = os.getenv(
    "GTFS_FV_URL", "https://download.gtfs.de/germany/fv_free/latest.zip"
)
GTFS_REFRESH_DAYS = int(os.getenv("GTFS_REFRESH_DAYS", "7"))
MATCH_WINDOW_MINUTES = 10
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Categorias de larga distancia: se enriquecen con el feed 'fv'.
LONG_DISTANCE_CATEGORIES = {
    "ICE",
    "IC",
    "EC",
    "ECE",
    "EN",
    "RJ",
    "RJB",
    "NJ",
    "TGV",
    "THA",
    "PE",
    "FLX",
    "FLX10",
}

_FEEDS = {
    "rv": (GTFS_RV_URL, DATA_DIR / "gtfs_rv" / "latest.zip", DATA_DIR / "gtfs_rv.json"),
    "fv": (GTFS_FV_URL, DATA_DIR / "gtfs_fv" / "latest.zip", DATA_DIR / "gtfs_fv.json"),
}


def is_regional(category: str) -> bool:
    """True si el tren es regional (se enriquece con el feed 'rv')."""
    return category.upper() not in LONG_DISTANCE_CATEGORIES


def _download(url: str, zip_path: Path) -> bool:
    try:
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        part = zip_path.with_suffix(".zip.part")
        urllib.request.urlretrieve(url, part)  # nosec: B310 (fuente fija)
        os.replace(part, zip_path)
        return True
    except Exception:  # noqa: BLE001
        return False


def _station_stop_ids(zip_path: Path, station_name: str = "Fulda") -> set[str]:
    """Paradas cuya nombre contiene la estacion (minusculas)."""
    token = station_name.lower()
    wanted: set[str] = set()
    try:
        with zipfile.ZipFile(zip_path) as z:
            with io.TextIOWrapper(z.open("stops.txt"), encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if token in row.get("stop_name", "").lower():
                        wanted.add(row["stop_id"])
    except Exception:  # noqa: BLE001
        return wanted
    return wanted


def _build_index(zip_path: Path, station_name: str = "Fulda") -> list[dict]:
    """Eventos en la estacion del feed: hora, linea y destino (headsign)."""
    stop_ids = _station_stop_ids(zip_path, station_name)
    if not stop_ids:
        return []

    routes: dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as z:
        with io.TextIOWrapper(z.open("routes.txt"), encoding="utf-8") as f:
            for row in csv.DictReader(f):
                routes[row["route_id"]] = row.get("route_short_name", "")

        trips: dict[str, tuple[str, str]] = {}
        with io.TextIOWrapper(z.open("trips.txt"), encoding="utf-8") as f:
            for row in csv.DictReader(f):
                route_id = row.get("route_id", "")
                trips[row.get("trip_id", "")] = (
                    route_id,
                    routes.get(route_id, ""),
                )

        events: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        with io.TextIOWrapper(z.open("stop_times.txt"), encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("stop_id") not in stop_ids:
                    continue
                dep = row.get("departure_time") or row.get("arrival_time") or ""
                if not dep:
                    continue
                _line = trips.get(row.get("trip_id", ""), ("", ""))[1]
                dest = row.get("stop_headsign", "") or ""
                key = (dep, _line, dest)
                if key in seen:
                    continue
                seen.add(key)
                events.append({"dep": dep, "line": _line, "dest": dest})
    events.sort(key=lambda e: e["dep"])
    return events


def _minutes(dep: str) -> int:
    parts = dep.split(":")[:2]
    try:
        return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        return -1


def _ensure_index(feed: str, station_name: str) -> list[dict]:
    url, zip_path, index_path = _FEEDS[feed]
    try:
        if not zip_path.exists() or (
            index_path.exists()
            and index_path.stat().st_mtime
            < time.time() - GTFS_REFRESH_DAYS * 86400
        ):
            if not _download(url, zip_path):
                return []
        if not index_path.exists() or (
            index_path.stat().st_mtime < zip_path.stat().st_mtime
        ):
            events = _build_index(zip_path, station_name)
            if events:
                index_path.write_text(
                    json.dumps(events, ensure_ascii=False), encoding="utf-8"
                )
        if index_path.exists():
            return json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        pass
    return []


def load_regional_index(station_name: str = "Fulda") -> list[dict]:
    """Indice del feed GTFS-DE regional (rv)."""
    return _ensure_index("rv", station_name)


def load_long_distance_index(station_name: str = "Fulda") -> list[dict]:
    """Indice del feed GTFS-DE de larga distancia (fv: ICE/IC/EC...)."""
    return _ensure_index("fv", station_name)


def match_events(index: list[dict], when: str) -> tuple[str, str]:
    """Empareja por hora (+-10 min) el evento mas proximo.

    Devuelve ``(linea, destino)``.
    """
    target = _minutes(when)
    if target < 0:
        return "", ""
    best = None
    best_diff = MATCH_WINDOW_MINUTES + 1
    for event in index:
        diff = abs(_minutes(event["dep"]) - target)
        if diff > MATCH_WINDOW_MINUTES:
            continue
        if diff < best_diff:
            best, best_diff = event, diff
    if best is None:
        return "", ""
    return best["line"], best["dest"]