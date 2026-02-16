@echo off
setlocal

if "%~1"=="" (
  echo Usage: %~nx0 "D:\path\to\dataset"
  exit /b 2
)

set "TARGET=%~1"
if not exist "%TARGET%" (
  echo Folder not found: "%TARGET%"
  exit /b 2
)

echo ===============================
echo  CLEAN TXT / NEEDTAG
echo ===============================
echo Target: "%TARGET%"

set /a TXT_COUNT=0
for /f %%C in ('dir /s /b "%TARGET%\*.txt" 2^>nul ^| find /c /v ""') do set TXT_COUNT=%%C
set /a NEEDTAG_COUNT=0
for /f %%C in ('dir /s /b "%TARGET%\*.needtag" 2^>nul ^| find /c /v ""') do set NEEDTAG_COUNT=%%C

echo .txt count     : %TXT_COUNT%
echo .needtag count : %NEEDTAG_COUNT%

echo Type YES to delete these files.
set /p CONFIRM=>
if /i not "%CONFIRM%"=="YES" (
  echo Cancelled.
  exit /b 1
)

del /s /q "%TARGET%\*.txt" 2>nul
del /s /q "%TARGET%\*.needtag" 2>nul

echo Deleted.
exit /b 0
