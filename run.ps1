param(
    [switch]$NoBrowser
)

# --------------------------------------------------------------------------- #
# run.ps1 - Arranque automatico del MVP en Windows (PowerShell).
#
# Levanta Docker/Kafka, prepara el entorno Python, arranca todos los procesos
# en segundo plano y abre el dashboard en el navegador.
# --------------------------------------------------------------------------- #

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

function Step($msg) { Write-Host "[start] $msg" -ForegroundColor Cyan }

# --------------------------------------------------------------------------- #
# 1. Docker Desktop
# --------------------------------------------------------------------------- #
docker info *> $null
if (-not $?) {
    Step "Docker no esta en marcha. Arrancando Docker Desktop..."
    $candidates = @(
        "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe",
        "$env:LOCALAPPDATA\Programs\DockerDesktop\Docker Desktop.exe"
    )
    $exe = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($exe) { Start-Process $exe }
    $ok = $false
    for ($i = 0; $i -lt 60; $i++) {
        docker info *> $null
        if ($?) { $ok = $true; break }
        Start-Sleep -Seconds 5
    }
    if (-not $ok) {
        Write-Host "[start] ERROR: Docker no respondio. Abre Docker Desktop manualmente." -ForegroundColor Red
        exit 1
    }
}
Step "Docker listo."

# --------------------------------------------------------------------------- #
# 2. Kafka
# --------------------------------------------------------------------------- #
Step "Levantando Kafka..."
& docker compose up -d | Out-Null

$kafka = $false
for ($i = 0; $i -lt 60; $i++) {
    & docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list *> $null
    if ($?) { $kafka = $true; break }
    Start-Sleep -Seconds 3
}
if (-not $kafka) {
    Write-Host "[start] ERROR: Kafka no respondio en localhost:9092." -ForegroundColor Red
    exit 1
}
Step "Kafka listo."

foreach ($topic in @("train-telemetry", "passenger-itineraries", "operational-incidents")) {
    & docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 `
        --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1 *> $null
}
Step "Topics verificados."

# --------------------------------------------------------------------------- #
# 3. Entorno Python
# --------------------------------------------------------------------------- #
$py = "$root\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    Step "Creando entorno virtual..."
    python -m venv "$root\.venv"
}

& $py -c "import streamlit, kafka, pydantic, google.transit" 2>$null
if (-not $?) {
    Step "Instalando dependencias (puede tardar la primera vez)..."
    & $py -m pip install --upgrade pip *> $null
    & $py -m pip install -r requirements.txt
}
Step "Entorno Python listo."

# --------------------------------------------------------------------------- #
# 3b. Fuente de datos (mock = demo local, db = API DB Timetables en vivo)
# --------------------------------------------------------------------------- #
$dataSource = "mock"
$envFile = Join-Path $root ".env"
if (Test-Path $envFile) {
    $m = Select-String -Path $envFile -Pattern '^\s*DATA_SOURCE\s*=\s*(\S+)' | Select-Object -First 1
    if ($m) { $dataSource = $m.Matches[0].Groups[1].Value.Trim().Trim('"').ToLower() }
}
Step "Fuente de datos: $dataSource"

# --------------------------------------------------------------------------- #
# 4. Arrancar procesos
# --------------------------------------------------------------------------- #
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
$procs = @()

function Start-Comp($name, $arguments) {
    $p = Start-Process -FilePath $py -ArgumentList $arguments -WorkingDirectory $root `
        -RedirectStandardOutput "$root\logs\$name.out" `
        -RedirectStandardError "$root\logs\$name.err" `
        -WindowStyle Hidden -PassThru
    $script:procs += $p
    return $p
}

Step "Iniciando consola de incidencias (operario)..."
Start-Comp "incidents" @("-u", "src\consumers\incident_console.py") | Out-Null

if ($dataSource -eq "nationwide") {
    Step "Modo nacional: GTFS-RT de toda Alemania (realtime.gtfs.de)."
    Step "Iniciando productor nacional (la primera vez descarga el horario fv)..."
    Start-Comp "producer" @("-u", "src\producers\gtfsrt_nationwide_producer.py") | Out-Null
} elseif ($dataSource -eq "db") {
    Step "Modo datos reales: se omite el mock GTFS-RT y los itinerarios de demo."
    Step "Iniciando productor de datos reales de DB (Fulda)..."
    Start-Comp "producer" @("-u", "src\producers\db_timetables_producer.py") | Out-Null
} else {
    Step "Iniciando servidor GTFS-RT simulado..."
    Start-Comp "mock" @("-u", "src\mock_gtfs_server.py") | Out-Null
    Start-Sleep -Seconds 3

    Step "Publicando itinerarios de pasajeros..."
    & $py -u src\producers\passenger_producer.py

    Step "Iniciando productor GTFS-RT..."
    Start-Comp "producer" @("-u", "src\producers\gtfs_producer.py") | Out-Null
}

Step "Iniciando detector de transbordos..."
Start-Comp "detector" @("-u", "src\processors\connection_detector.py") | Out-Null
Start-Sleep -Seconds 2

Step "Iniciando dashboard web (Streamlit)..."
Start-Comp "dashboard" @(
    "-u", "-m", "streamlit", "run", "src\dashboard_app.py",
    "--server.headless", "true",
    "--server.port", "8501",
    "--browser.gatherUsageStats", "false"
) | Out-Null

# Guardar PIDs para poder detenerlos con stop.bat
$procs | ForEach-Object { $_.Id } | Set-Content "$root\logs\pids.txt"

# --------------------------------------------------------------------------- #
# 5. Esperar al dashboard y abrir el navegador
# --------------------------------------------------------------------------- #
Step "Esperando al dashboard..."
$up = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8501/_stcore/health" -UseBasicParsing -TimeoutSec 3
        if ($r.Content -match "ok") { $up = $true; break }
    } catch {}
    Start-Sleep -Seconds 2
}

if ($up) {
    Step "Dashboard listo en http://localhost:8501"
    if (-not $NoBrowser) { Start-Process "http://localhost:8501" }
} else {
    Write-Host "[start] El dashboard aun no responde; revisa logs\dashboard.err" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "[start] Sistema en marcha." -ForegroundColor Green
Write-Host "[start] Para detenerlo, ejecuta stop.bat (o cierra la sesion)." -ForegroundColor Green
