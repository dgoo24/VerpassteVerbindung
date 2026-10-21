"""Logica de negocio pura (sin dependencia de Kafka ni de la interfaz).

Aisla el calculo de severidad y la accion recomendada para que puedan usarse
tanto desde el procesador (connection_detector) como desde el dashboard, sin
acoplar las reglas a la UI.
"""

from __future__ import annotations

from dataclasses import dataclass

SEVERITY_ALTA = "HOCH"
SEVERITY_MEDIA = "MITTEL"
SEVERITY_BAJA = "NIEDRIG"

# Versiones anteriores en espanol, para compatibilidad con datos ya emitidos.
_LEGACY = {"ALTA": SEVERITY_ALTA, "MEDIA": SEVERITY_MEDIA, "BAJA": SEVERITY_BAJA}

# Orden de prioridad para presentacion (menor = mas urgente).
SEVERITY_ORDER = {SEVERITY_ALTA: 0, SEVERITY_MEDIA: 1, SEVERITY_BAJA: 2}


def severity_label(value: str) -> str:
    """Normaliza la severidad a la etiqueta actual (traduce valores viejos)."""
    return _LEGACY.get(value, value)


@dataclass(frozen=True)
class SeverityVerdict:
    """Resultado del calculo de severidad de una incidencia."""

    severity: str
    recommended_action: str


# Umbrales sobre el deficit de tiempo (min requeridos - min disponibles).
_DEFICIT_ALTA_MIN = 10.0
_DEFICIT_MEDIA_MIN = 5.0

# Umbrales sobre el retraso del tren de origen (segundos).
_DELAY_ALTA_SECONDS = 20 * 60
_DELAY_MEDIA_SECONDS = 10 * 60

_ACTIONS = {
    SEVERITY_ALTA: "Anschluss halten / Bahnhof informieren und Gleis neu zuweisen",
    SEVERITY_MEDIA: "Bahnhof informieren und schnellen Umstieg vorbereiten",
    SEVERITY_BAJA: "Reisende informieren und beobachten",
}


def evaluate_incident_severity(
    deficit_minutes: float,
    delay_seconds: int = 0,
) -> SeverityVerdict:
    """Calcula la severidad y la accion recomendada de una incidencia.

    El deficit (cuanto falta para garantizar el transbordo) es el criterio
    principal; el retraso del tren de origen actua como refuerzo.
    """
    if deficit_minutes >= _DEFICIT_ALTA_MIN or delay_seconds >= _DELAY_ALTA_SECONDS:
        return SeverityVerdict(SEVERITY_ALTA, _ACTIONS[SEVERITY_ALTA])
    if deficit_minutes >= _DEFICIT_MEDIA_MIN or delay_seconds >= _DELAY_MEDIA_SECONDS:
        return SeverityVerdict(SEVERITY_MEDIA, _ACTIONS[SEVERITY_MEDIA])
    return SeverityVerdict(SEVERITY_BAJA, _ACTIONS[SEVERITY_BAJA])


def severity_rank(severity: str) -> int:
    """Devuelve el orden de prioridad (ALTA=0, MEDIA=1, BAJA=2)."""
    return SEVERITY_ORDER.get(severity, len(SEVERITY_ORDER))
