@echo off
title MVP Transbordos - Iniciando
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run.ps1"
echo.
echo Puedes cerrar esta ventana. El sistema sigue en marcha.
timeout /t 5 >nul
