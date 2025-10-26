@echo off
REM ORCA Pipeline Background Execution Script
REM For Windows

if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="status" goto status
if "%1"=="log" goto log
goto usage

:start
echo Starting ORCA pipeline in background...
start /B pythonw main.py
echo Pipeline started in background
echo To stop, use: run_background.bat stop
goto end

:stop
echo Stopping pipeline...
taskkill /F /IM pythonw.exe /FI "WINDOWTITLE eq main.py"
taskkill /F /IM python.exe /FI "WINDOWTITLE eq main.py"
echo Pipeline stopped
goto end

:status
echo Checking pipeline status...
tasklist /FI "IMAGENAME eq pythonw.exe" /FI "WINDOWTITLE eq main.py" | find "pythonw.exe" >nul
if %errorlevel%==0 (
    echo Pipeline is running
) else (
    echo Pipeline is not running
)
goto end

:log
echo Showing latest log file...
for /f "delims=" %%i in ('dir /b /od logs\pipeline_*.log') do set LATEST=%%i
if defined LATEST (
    type logs\%LATEST%
) else (
    echo No log files found
)
goto end

:usage
echo Usage: run_background.bat [start^|stop^|status^|log]
echo.
echo Commands:
echo   start   - Start pipeline in background
echo   stop    - Stop pipeline
echo   status  - Check if pipeline is running
echo   log     - View latest log file
goto end

:end
