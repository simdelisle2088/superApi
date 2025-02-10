@echo off

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Define values for parameters
set HOST=0.0.0.0
set PORT=8001
set LOOP=asyncio
set HTTP=h11
set LOG_LEVEL=info

REM Check Uvicorn version
uvicorn --version

REM Start Uvicorn server
uvicorn geo:app ^
    --host %HOST% ^
    --port %PORT% ^
    --loop %LOOP% ^
    --http %HTTP% ^
    --log-level %LOG_LEVEL% ^
    --reload ^
    --no-use-colors ^
    --proxy-headers ^
    --no-server-header ^
    --no-date-header
