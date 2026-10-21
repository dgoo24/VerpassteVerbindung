# --------------------------------------------------------------------------- #
# stop.ps1 - Detiene los procesos del MVP y apaga Kafka.
# --------------------------------------------------------------------------- #

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "[stop] Deteniendo procesos..."

# 1. Matar los arboles de proceso guardados en pids.txt (lanzador + hijos).
if (Test-Path "$root\logs\pids.txt") {
    Get-Content "$root\logs\pids.txt" | ForEach-Object {
        if ($_ -match "^\d+$") {
            & taskkill /PID ([int]$_) /T /F *> $null
        }
    }
    Remove-Item "$root\logs\pids.txt" -Force -ErrorAction SilentlyContinue
}

# 2. Red de seguridad: cualquier Python del entorno virtual que siga vivo
#    (mata tambien sus procesos hijos).
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.ExecutablePath -like "$root\.venv\*" } |
    ForEach-Object { & taskkill /PID $_.ProcessId /T /F *> $null }

Write-Host "[stop] Apagando Kafka..."
& docker compose down | Out-Null

Write-Host "[stop] Sistema detenido."
