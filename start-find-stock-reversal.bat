@echo off
title NSE ^& BSE Stock Screener - Shashikant Gaikwad
color 0A

echo =====================================================================
echo           NSE ^& BSE Quantitative Stock Screener
echo                   Owner: Shashikant Gaikwad
echo =====================================================================
echo.

:: 1. Verify Python installation
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python was not found in your system PATH.
    echo Please install Python 3.9+ from https://www.python.org/ and check "Add to PATH".
    echo.
    pause
    exit /b
)

:: 2. Ensure execution directory matches this batch file location
cd /d "%~dp0"

:: 3. Check if app.py exists
if not exist "app.py" (
    echo [ERROR] app.py not found in "%~dp0".
    echo Please make sure this .bat file is placed in the exact same folder as app.py.
    echo.
    pause
    exit /b
)

echo [1/3] Checking required libraries (FastAPI, Uvicorn, Pandas, YFinance)...
python -m pip install --quiet fastapi uvicorn requests pandas numpy yfinance

echo [2/3] Starting backend engine on http://127.0.0.1:8000...
start /b "" python app.py

echo [3/3] Waiting for server initialization...
timeout /t 3 /nobreak >nul

echo.
echo Launching screener in your default web browser...
start http://127.0.0.1:8000/

echo.
echo =====================================================================
echo Screener is active at: http://127.0.0.1:8000/
echo Keep this terminal window open while using the application.
echo To shut down the server, press Ctrl + C or simply close this window.
echo =====================================================================
echo.

:: Keep command window open to show background logs and status
python -c "import time; [time.sleep(3600) for _ in iter(int, 1)]"