@echo off
cd /d "%~dp0"
if not exist ".env" copy ".env.example" ".env" >nul
start "" notepad ".env"
echo Set PGPASSWORD to your PostgreSQL password, save the file, then close Notepad.
pause
