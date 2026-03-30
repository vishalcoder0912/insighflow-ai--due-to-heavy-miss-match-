@echo off
echo ========================================
echo  InsightFlow AI - Starting Services
echo ========================================
echo.

REM Set Python path
set PYTHON=C:\Users\VISHAL\AppData\Local\Programs\Python\Python314\python.exe

REM Start Backend in background
echo [1/2] Starting Backend on port 8001...
cd /d "%~dp0backend"
start "Backend Server" "%PYTHON%" -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8001

REM Wait for backend to start
timeout /t 3 /nobreak > nul

REM Start Frontend
echo [2/2] Starting Frontend on port 8003...
cd /d "%~dp0frontend"
start "Frontend Server" cmd /c "npm run dev"

echo.
echo ========================================
echo  Both servers are starting!
echo  Frontend: http://localhost:8003
echo  Backend:  http://localhost:8001
echo  API Docs: http://localhost:8001/docs
echo ========================================
echo.
echo Press any key to open the application...
pause > nul
start http://localhost:8003
