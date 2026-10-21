"""Consola de incidencias del centro de control (operario).

Consume el topic operational-incidents y muestra por consola una pizarra
priorizada de las incidencias de red: severidad, estacion, enlace afectado,
pasajeros afectados, deficit de tiempo y accion recomendada.
"""

from __future__ import annotations

import json
import os
import sys
import time

from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from business_rules import (  # noqa: E402
    SEVERITY_ALTA,
    SEVERITY_BAJA,
    SEVERITY_MEDIA,
)
from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_OPERATIONAL_INCIDENTS,
)

# Codigos ANSI con la paleta de la casa (DB-Rot #EC0016, ambar, azul info).
_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2;37m"
_DB_WHITE = "\033[97m"
_DB_RED_FG = "\033[38;5;196m"      # rojo DB sobre fondo claro
_DB_RED_BG = "\033[48;5;196m"      # fondo rojo DB
_DB_AMBER_BG = "\033[48;5;214m"    # ambar de aviso
_DB_BLUE_BG = "\033[48;5;25m"      # azul informativo

_COLORS = {
    SEVERITY_ALTA: f"{_BOLD}{_DB_WHITE}{_DB_RED_BG}",
    SEVERITY_MEDIA: f"{_BOLD}\033[30m{_DB_AMBER_BG}",
    SEVERITY_BAJA: f"{_BOLD}{_DB_WHITE}{_DB_BLUE_BG}",
}

_SEPARADOR = "═" * 66


def _enable_ansi() -> None:
    """Activa el procesamiento VT en Windows 10+ para ver los colores."""
    if os.name != "nt":
        return
    try:  # pragma: no cover - dependiente del host de consola
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    except Exception:  # noqa: BLE001
        pass


def _label(severity: str) -> str:
    color = _COLORS.get(severity, "")
    return f"{color} {severity} {_RESET}" if color else severity


def _field(name: str, value: object) -> str:
    """Fila de la ficha con la etiqueta en gris y el valor en blanco."""
    return f"{_DIM}{name:<28}{_RESET}{_DB_WHITE}{value}{_RESET}"


def render_incident(incident: dict) -> str:
    """Construye la ficha visual de una incidencia operativa."""
    delay_min = round(incident.get("delay_seconds", 0) / 60.0, 1)
    low = incident.get("passengers_affected_low")
    high = incident.get("passengers_affected_high")
    mid = incident.get("passengers_affected")
    affected = f"{low}-{high} (est.)" if low and high and low != high else mid
    severity = incident.get("severity", "")
    title = f"{_BOLD}{_DB_RED_FG}◤ {_label(severity)} — INCIDENCIA OPERATIVA ◢{_RESET}"
    rows = [
        _field("Severidad", _label(severity)),
        _field("Estacion", incident.get("station_id")),
        _field("Tren con retraso (origen)", incident.get("origin_trip")),
        _field("Tren en riesgo (destino)", incident.get("destination_trip")),
        _field("Pasajeros afectados", affected),
        _field("Retraso origen", f"{delay_min} min"),
        _field("Tiempo disponible", f"{incident.get('available_transfer_minutes')} min"),
        _field("Tiempo requerido", f"{incident.get('required_minutes')} min"),
        _field(
            "Deficit",
            f"{_BOLD}{_DB_RED_FG}{incident.get('deficit_minutes')} min{_RESET}",
        ),
        _field("Accion recomendada", incident.get("recommended_action")),
        _field("Registrada", incident.get("created_at")),
    ]
    body = "\n".join(rows)
    width = max(len(_SEPARADOR), len(title) - 9)
    footer = f"{_DB_RED_FG}{_SEPARADOR[:width]}{_RESET}"
    return "\n".join([title, footer, body, footer])


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_OPERATIONAL_INCIDENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="incident-console",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main() -> None:
    _enable_ansi()
    print(f"{_DB_RED_FG}{_SEPARADOR}{_RESET}")
    print(f"{_BOLD}{_DB_WHITE}  DB · CONSOLA DE INCIDENCIAS - CENTRO DE CONTROL{_RESET}")
    print(f"{_DIM}  Escuchando '{TOPIC_OPERATIONAL_INCIDENTS}'...{_RESET}")
    print(f"{_DB_RED_FG}{_SEPARADOR}{_RESET}")

    # Bucle de reconexion: ante un fallo del consumidor se recrea y continua.
    while True:
        consumer = _build_consumer()
        try:
            for message in consumer:
                print()
                print(render_incident(message.value))
        except KeyboardInterrupt:
            print("\n[incident-console] Deteniendo...")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[incident-console] Error de consumo, reconectando: {exc}")
            time.sleep(3)
        finally:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    main()
