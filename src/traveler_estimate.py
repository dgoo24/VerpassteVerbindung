"""Estimacion de viajeros de una conexion por categoria de tren.

GTFS-RT no publica ocupacion real, asi que la demanda de transbordo se modela
como una fraccion de la capacidad tipica de una unidad por categoria. Se
devuelve siempre una tripleta (baja, media, alta) para comunicar la
incertidumbre en vez de un numero exacto.
"""

from __future__ import annotations

TRAIN_CAPACITY_BY_CATEGORY: dict[str, int] = {
    "ICE": 445,
    "IC": 380,
    "EC": 340,
    "ECE": 340,
    "TGV": 500,
    "THA": 500,
    "NJ": 380,
    "EN": 380,
    "RJ": 380,
    "FLX": 400,
}
REGIONAL_CAPACITY = 220
CONNECTION_LOAD_FACTOR = 0.5
CONNECTION_LOAD_FACTOR_LOW = 0.35
CONNECTION_LOAD_FACTOR_HIGH = 0.65


def estimate_connection_travelers(
    category: str, load_factor: float = CONNECTION_LOAD_FACTOR
) -> tuple[int, int, int]:
    """(baja, media, alta) de viajeros de un transbordo por tren de origen.

    Punto medio = factor de carga (50%) de la capacidad tipica; el rango lo
    abre a 35%-65%.
    """
    for prefix, capacity in TRAIN_CAPACITY_BY_CATEGORY.items():
        if category.upper().startswith(prefix):
            low = max(1, int(capacity * CONNECTION_LOAD_FACTOR_LOW))
            mid = max(low, int(capacity * load_factor))
            high = max(mid, int(capacity * CONNECTION_LOAD_FACTOR_HIGH))
            return low, mid, high
    low = max(1, int(REGIONAL_CAPACITY * CONNECTION_LOAD_FACTOR_LOW))
    mid = max(low, int(REGIONAL_CAPACITY * load_factor))
    high = max(mid, int(REGIONAL_CAPACITY * CONNECTION_LOAD_FACTOR_HIGH))
    return low, mid, high
