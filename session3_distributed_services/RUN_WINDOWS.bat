@echo off
cd /d "%~dp0"
if not exist ".env" (
  echo Missing .env file. Copy .env.example to .env and set PGPASSWORD first.
  pause
  exit /b 1
)
if exist ".venv\Scripts\python.exe" (
  set PY=.venv\Scripts\python.exe
) else (
  set PY=python
)
%PY% -m pip install -r requirements.txt
%PY% database.py
if errorlevel 1 pause & exit /b 1
%PY% contracts.py
%PY% test_wire.py
%PY% run_pipeline.py
pause
