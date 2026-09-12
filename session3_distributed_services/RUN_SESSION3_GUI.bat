@echo off
cd /d "%~dp0"
if not exist ".env" (
  echo Missing .env file.
  echo Copy .env.example to .env and set PGPASSWORD first.
  pause
  exit /b 1
)
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" session3_dashboard.py
) else (
  python session3_dashboard.py
)
if errorlevel 1 pause
