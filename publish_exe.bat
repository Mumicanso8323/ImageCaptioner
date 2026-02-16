@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM publish_exe.bat (log always, robust working directory)
REM =========================================================

REM 1) Ensure we run in the folder where this .bat lives
pushd "%~dp0" 1>nul 2>nul
if errorlevel 1 (
  echo [ERROR] Failed to set working directory to "%~dp0"
  pause
  exit /b 1
)

REM 2) Prepare log directory
if not exist "log" mkdir "log" >nul 2>nul

REM 3) Timestamp (locale-safe) for logfile name
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "LOGFILE=log\publish_exe_%TS%.log"

echo [INFO] WorkingDir: "%CD%" > "%LOGFILE%"
echo [INFO] LogFile   : "%LOGFILE%" >> "%LOGFILE%"
echo [INFO] StartTime : %date% %time% >> "%LOGFILE%"
echo. >> "%LOGFILE%"

REM 4) Sanity checks
where dotnet >> "%LOGFILE%" 2>&1
if errorlevel 1 (
  echo [ERROR] dotnet not found. >> "%LOGFILE%"
  echo [ERROR] dotnet が見つかりません。Visual Studio / .NET SDK を確認してください。
  echo --- log: "%LOGFILE%"
  popd
  pause
  exit /b 1
)

if not exist "ImageCaptioner.csproj" (
  echo [ERROR] ImageCaptioner.csproj not found in "%CD%". >> "%LOGFILE%"
  echo [ERROR] ImageCaptioner.csproj が見つかりません。ショートカットの「作業フォルダ」ズレの可能性大。
  echo --- log: "%LOGFILE%"
  popd
  pause
  exit /b 1
)

REM 5) Run publish (capture stdout+stderr)
echo [INFO] Running dotnet publish... >> "%LOGFILE%"
echo -------------------------------------------------------- >> "%LOGFILE%"
dotnet publish "ImageCaptioner.csproj" -c Release -r win-x64 --self-contained true ^
  /p:PublishSingleFile=true ^
  /p:EnableCompressionInSingleFile=true ^
  /p:IncludeNativeLibrariesForSelfExtract=true >> "%LOGFILE%" 2>&1
set "EC=%errorlevel%"
echo -------------------------------------------------------- >> "%LOGFILE%"
echo [INFO] ExitCode: !EC! >> "%LOGFILE%"

if not "!EC!"=="0" (
  echo [ERROR] publish failed (ExitCode=!EC!)
  echo --- log: "%LOGFILE%"
  popd
  pause
  exit /b !EC!
)

REM 6) Print output path (and log it)
set "EXE_PATH=bin\Release\net8.0\win-x64\publish\ImageCaptioner.exe"
if exist "!EXE_PATH!" (
  echo [OK] EXE: "!EXE_PATH!"
  echo [OK] EXE: "!EXE_PATH!" >> "%LOGFILE%"
) else (
  echo [WARN] Publish succeeded but EXE not found at expected path:
  echo        "!EXE_PATH!"
  echo [WARN] EXE not found: "!EXE_PATH!" >> "%LOGFILE%"
)

echo [INFO] EndTime: %date% %time% >> "%LOGFILE%"
echo --- log: "%LOGFILE%"

popd
pause
exit /b 0
