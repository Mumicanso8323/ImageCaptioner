@echo off
setlocal EnableExtensions DisableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Failed to switch working directory to "%SCRIPT_DIR%".
  call :maybe_pause
  exit /b 1
)

if not exist "log" mkdir "log" >nul 2>&1
call :timestamp
set "LOG_FILE=log\tool_%STAMP%.log"

set "CMD=%~1"
if "%CMD%"=="" set "CMD=publish"

call :log "[INFO] WorkingDir: %CD%"
call :log "[INFO] LogFile: %LOG_FILE%"
call :log "[INFO] StartTime: %DATE% %TIME%"
call :log "[INFO] Command: %CMD%"

where dotnet >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :log "[ERROR] dotnet SDK not found in PATH."
  echo [ERROR] dotnet SDK not found.
  echo --- log: "%LOG_FILE%"
  popd
  call :maybe_pause
  exit /b 1
)

if not exist "ImageCaptioner.sln" (
  call :log "[ERROR] ImageCaptioner.sln not found in %CD%."
  echo [ERROR] ImageCaptioner.sln not found.
  echo --- log: "%LOG_FILE%"
  popd
  call :maybe_pause
  exit /b 1
)

if /i "%CMD%"=="build" goto do_build
if /i "%CMD%"=="publish" goto do_publish
if /i "%CMD%"=="restore" goto do_restore

echo Usage: tool.bat [restore^|build^|publish]
call :log "[ERROR] Unknown command: %CMD%"
popd
call :maybe_pause
exit /b 2

:do_restore
call :run dotnet restore "ImageCaptioner.sln"
set "EXIT_CODE=%ERRORLEVEL%"
goto finish

:do_build
call :run dotnet restore "ImageCaptioner.sln"
if errorlevel 1 goto fail
call :run dotnet build "ImageCaptioner.sln" -c Release --no-restore
set "EXIT_CODE=%ERRORLEVEL%"
goto finish

:do_publish
if not exist "dist" mkdir "dist" >nul 2>&1
if not exist "dist\app" mkdir "dist\app" >nul 2>&1
call :run dotnet restore "ImageCaptioner.sln"
if errorlevel 1 goto fail
call :run dotnet publish "ImageCaptioner.csproj" -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true /p:EnableCompressionInSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true -o "dist\app"
if errorlevel 1 goto fail
if exist "third_party" (
  xcopy /E /I /Y "third_party" "dist\app\third_party" >> "%LOG_FILE%" 2>&1
  if errorlevel 1 goto fail
)
set "EXIT_CODE=0"
goto finish

:fail
set "EXIT_CODE=%ERRORLEVEL%"

:finish
call :log "[INFO] EndTime: %DATE% %TIME%"
call :log "[INFO] ExitCode: %EXIT_CODE%"
if not "%EXIT_CODE%"=="0" (
  echo [ERROR] tool.bat failed with ExitCode=%EXIT_CODE%.
  echo --- log: "%LOG_FILE%"
  popd
  call :maybe_pause
  exit /b %EXIT_CODE%
)

echo [OK] tool.bat %CMD% completed.
echo --- log: "%LOG_FILE%"
popd
exit /b 0

:run
setlocal
set "RUN_CMD=%*"
>> "%LOG_FILE%" echo [CMD] %RUN_CMD%
cmd /c "%RUN_CMD%" >> "%LOG_FILE%" 2>&1
set "RUN_EC=%ERRORLEVEL%"
>> "%LOG_FILE%" echo [CMD-EXIT] %RUN_EC%
endlocal & exit /b %RUN_EC%

:log
>> "%LOG_FILE%" echo %~1
exit /b 0

:timestamp
set "STAMP=%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "STAMP=%STAMP: =0%"
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "STAMP=%%i"
exit /b 0

:maybe_pause
set "PAUSE_ON_FAIL=0"
echo %CMDCMDLINE% | findstr /i /c:"/c" >nul && set "PAUSE_ON_FAIL=1"
if "%PAUSE_ON_FAIL%"=="1" pause
exit /b 0
