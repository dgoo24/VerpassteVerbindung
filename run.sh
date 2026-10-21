#!/usr/bin/env bash
#
# run.sh - Arranque completo del MVP de transbordos perdidos en tiempo real.
#
# Pasos:
#   1. Crear y activar un entorno virtual de Python.
#   2. Instalar las dependencias de requirements.txt.
#   3. Levantar Kafka (KRaft) con docker compose up -d.
#   4. Esperar a que Kafka escuche en el puerto 9092.
#   5. Arrancar la fuente de datos elegida (mock local, API DB o GTFS-RT
#      nacional de toda Alemania) y publicar telemetria e itinerarios.
#   6. Arrancar en paralelo: consola de incidencias (operario), detector,
#      productor y el dashboard web (Streamlit) en http://localhost:8501.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python"
fi

VENV_DIR=".venv"

# --------------------------------------------------------------------------- #
# 1. Entorno virtual
# --------------------------------------------------------------------------- #
if [ ! -d "$VENV_DIR" ]; then
  echo "[run.sh] Creando entorno virtual en '$VENV_DIR'..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

if [ -f "$VENV_DIR/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"
elif [ -f "$VENV_DIR/Scripts/activate" ]; then
  # shellcheck disable=SC1091
  source "$VENV_DIR/Scripts/activate"
fi
echo "[run.sh] Entorno virtual activado ($(python --version 2>&1))."

# --------------------------------------------------------------------------- #
# 2. Dependencias
# --------------------------------------------------------------------------- #
echo "[run.sh] Instalando dependencias..."
python -m pip install --upgrade pip >/dev/null
python -m pip install -r requirements.txt

# --------------------------------------------------------------------------- #
# 3. Infraestructura
# --------------------------------------------------------------------------- #
echo "[run.sh] Levantando Kafka con Docker Compose..."
docker compose up -d

# --------------------------------------------------------------------------- #
# 4. Esperar a que Kafka este listo
# --------------------------------------------------------------------------- #
echo "[run.sh] Esperando a que Kafka escuche en localhost:9092..."
KAFKA_READY=0
for _ in $(seq 1 60); do
  if (echo > /dev/tcp/127.0.0.1/9092) >/dev/null 2>&1; then
    KAFKA_READY=1
    break
  fi
  sleep 2
done

if [ "$KAFKA_READY" -ne 1 ]; then
  echo "[run.sh] ERROR: Kafka no respondio en el puerto 9092." >&2
  exit 1
fi
echo "[run.sh] Kafka esta listo."

# Crear los topics de forma explicita (tolerante a fallos).
for topic in train-telemetry passenger-itineraries operational-incidents; do
  docker compose exec -T kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done
echo "[run.sh] Topics verificados."

# --------------------------------------------------------------------------- #
# 4b. Fuente de datos (mock = demo local, db = API DB Timetables en vivo)
# --------------------------------------------------------------------------- #
DATA_SOURCE="mock"
if [ -f .env ]; then
  DATA_SOURCE="$(grep -E '^[[:space:]]*DATA_SOURCE[[:space:]]*=' .env | tail -n1 | cut -d= -f2- | tr -d '[:space:]"' || true)"
  if [ -z "$DATA_SOURCE" ]; then
    DATA_SOURCE="mock"
  fi
fi
echo "[run.sh] Fuente de datos: $DATA_SOURCE"

# --------------------------------------------------------------------------- #
# 5. Servidor GTFS-RT simulado (segundo plano)
# --------------------------------------------------------------------------- #
PIDS=()
cleanup() {
  echo ""
  echo "[run.sh] Deteniendo procesos..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

if [ "$DATA_SOURCE" = "nationwide" ]; then
  echo "[run.sh] Modo nacional: GTFS-RT de toda Alemania (realtime.gtfs.de)."
elif [ "$DATA_SOURCE" = "db" ]; then
  echo "[run.sh] Modo datos reales: se omite el mock GTFS-RT y los itinerarios de demo."
else
  echo "[run.sh] Iniciando servidor GTFS-RT simulado..."
  python src/mock_gtfs_server.py &
  PIDS+=($!)

  sleep 2

  # ------------------------------------------------------------------------- #
  # 6. Poblar itinerarios
  # ------------------------------------------------------------------------- #
  echo "[run.sh] Publicando itinerarios de pasajeros..."
  python src/producers/passenger_producer.py
fi

# --------------------------------------------------------------------------- #
# 7. Procesos en paralelo
# --------------------------------------------------------------------------- #
echo "[run.sh] Iniciando consola de incidencias (operario)..."
python src/consumers/incident_console.py &
PIDS+=($!)
sleep 1

echo "[run.sh] Iniciando detector de transbordos..."
python src/processors/connection_detector.py &
PIDS+=($!)
sleep 1

if [ "$DATA_SOURCE" = "nationwide" ]; then
  echo "[run.sh] Iniciando productor nacional (la primera vez descarga el horario fv)..."
  python src/producers/gtfsrt_nationwide_producer.py &
  PIDS+=($!)
elif [ "$DATA_SOURCE" = "db" ]; then
  echo "[run.sh] Iniciando productor de datos reales de DB (Fulda)..."
  python src/producers/db_timetables_producer.py &
  PIDS+=($!)
else
  echo "[run.sh] Iniciando productor GTFS-RT..."
  python src/producers/gtfs_producer.py &
  PIDS+=($!)
fi

echo "[run.sh] Iniciando dashboard web (Streamlit)..."
python -m streamlit run src/dashboard_app.py \
  --server.headless true \
  --server.port 8501 \
  --browser.gatherUsageStats false &
PIDS+=($!)

echo ""
echo "[run.sh] Sistema en marcha. Pulsa Ctrl+C para detener."
echo "[run.sh] Dashboard disponible en http://localhost:8501"
echo ""
wait
