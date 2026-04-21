@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

REM Activate venv
if not exist ".\venv\Scripts\activate.bat" (
  echo [ERROR] Could not find .\venv\Scripts\activate.bat
  exit /b 1
)
call ".\venv\Scripts\activate.bat"

REM Validate files
if not exist ".\PeerInfo.txt" (
  echo [ERROR] Could not find PeerInfo.txt in %cd%
  exit /b 1
)

if not exist ".\Common.txt" (
  echo [ERROR] Could not find Common.txt in %cd%
  exit /b 1
)

if not exist ".\peerProcess.py" (
  echo [ERROR] Could not find peerProcess.py in %cd%
  exit /b 1
)

echo Launching nodes from PeerInfo.txt...
echo.

REM Parse PeerInfo.txt and launch each node
for /f "usebackq tokens=1,2,3,4 delims= " %%A in (".\PeerInfo.txt") do (
  set "FIRST=%%A"
  if not "!FIRST!"=="" (
    if not "!FIRST:~0,1!"=="#" (
      set "ID=%%A"
      set "IP=%%B"
      set "PORT=%%C"
      set "HASFILE=%%D"

      echo [START] id=!ID! ip=!IP! port=!PORT! hasFile=!HASFILE!
      start "Node_!ID!" /D "%cd%" cmd /k python ".\peerProcess.py" "!ID!"

      echo Waiting 0.5 seconds...
      timeout /t 1 /nobreak >nul
      REM (timeout only supports integer seconds; keeping your intent ~0.5s with 1s here)
      echo.
    )
  )
)

echo All processes started.
endlocal
