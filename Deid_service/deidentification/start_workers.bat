@echo off
setlocal enabledelayedexpansion

:: Number of workers to launch (defaults to 1)
set "count=%1"
if "%count%"=="" set count=1

:: Set your project and environment paths
set "PROJECT_DIR=D:\ssuman\deidentification\deIdentification"
set "CONDA_ENV_NAME=venv"

:: Launch each worker in a new Command Prompt window
for /L %%i in (1,1,%count%) do (
    start "Worker %%i" cmd /k "conda activate %CONDA_ENV_NAME% && cd /d %PROJECT_DIR% && python manage.py start_worker"
    timeout /t 1 >nul
)

echo %count% workers started in separate Command Prompt windows with conda environment activated.