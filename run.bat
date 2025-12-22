@echo off
setlocal enabledelayedexpansion

echo ======================================
echo  Hyperliquid Real-Time Data Collector
echo ======================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://www.python.org/
    pause
    exit /b 1
)

echo [✓] Python found
echo.

REM Check if config.json exists
if not exist "config.json" (
    echo WARNING: config.json not found
    echo Creating from config.example.json...
    if exist "config.example.json" (
        copy config.example.json config.json
        echo Please edit config.json with your Hyperliquid API credentials
        pause
    ) else (
        echo ERROR: config.example.json not found
        pause
        exit /b 1
    )
)

echo [✓] Configuration file ready
echo.

REM Create virtual environment if it doesn't exist
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo ERROR: Failed to create virtual environment
        pause
        exit /b 1
    )
    echo [✓] Virtual environment created
    echo.
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)
echo [✓] Virtual environment activated
echo.

REM Install requirements
echo Installing dependencies...
python -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)
echo [✓] Dependencies installed
echo.

REM Run the script
echo ======================================
echo Starting data collection...
echo ======================================
echo.
python realtime_data_HL.py

pause
