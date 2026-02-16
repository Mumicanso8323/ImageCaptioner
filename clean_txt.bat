@echo off
setlocal ENABLEDELAYEDEXPANSION

if "%~1"=="" (
  echo Usage: %~nx0 "D:\path\to\dataset"
  exit /b 2
)

set "TARGET_DIR=%~1"
if not exist "%TARGET_DIR%" (
  echo [error] Directory not found: "%TARGET_DIR%"
  exit /b 2
)

set /a TXT_COUNT=0
set /a NEEDTAG_COUNT=0
for /r "%TARGET_DIR%" %%F in (*.txt) do set /a TXT_COUNT+=1
for /r "%TARGET_DIR%" %%F in (*.needtag) do set /a NEEDTAG_COUNT+=1

echo Target: "%TARGET_DIR%"
echo .txt files to delete: !TXT_COUNT!
echo .needtag files to delete: !NEEDTAG_COUNT!
echo.
set /p CONFIRM=Type YES to proceed: 
if /I not "!CONFIRM!"=="YES" (
  echo Cancelled.
  exit /b 1
)

for /r "%TARGET_DIR%" %%F in (*.txt) do del /q "%%~fF"
for /r "%TARGET_DIR%" %%F in (*.needtag) do del /q "%%~fF"

echo Done.
exit /b 0
