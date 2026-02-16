@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem =========================================================
rem  clean_txt.bat
rem  - .txt / .needtag を削除
rem  - ログは ./log にタイムスタンプ付きで蓄積
rem =========================================================

set "SCRIPT_DIR=%~dp0"
set "LOG_DIR=%SCRIPT_DIR%log"
if not exist "%LOG_DIR%\" mkdir "%LOG_DIR%" >nul 2>&1

set "STAMP="
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss" 2^>nul') do set "STAMP=%%I"
if not defined STAMP set "STAMP=%DATE: =0%_%TIME: =0%"
set "STAMP=%STAMP::=%"
set "STAMP=%STAMP:/=%"
set "STAMP=%STAMP:.=%"

set "LOG_FILE=%LOG_DIR%\clean_txt_%STAMP%.log"

echo [info] log file: "%LOG_FILE%"
call :main %* 1>"%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo.
echo [info] 実行ログ: "%LOG_FILE%"
if not "%EXIT_CODE%"=="0" echo [error] clean_txt.bat は異常終了しました (exit code: %EXIT_CODE%)
echo.
pause
exit /b %EXIT_CODE%

:main
echo ==== START %DATE% %TIME% ====
echo Target cleanup tool
echo.

rem ===== 1) 対象ディレクトリの取得 =====
set "TARGET_DIR=%~1"
if "%TARGET_DIR%"=="" (
  set /p "TARGET_DIR=対象ディレクトリを入力（またはフォルダをドラッグ&ドロップしてEnter）: "
)

rem 入力が "..." で囲まれててもOK（先頭末尾の " を除去）
if not "%TARGET_DIR%"=="" (
  if "%TARGET_DIR:~0,1%"=="^"" set "TARGET_DIR=%TARGET_DIR:~1%"
  if "%TARGET_DIR:~-1%"=="^"" set "TARGET_DIR=%TARGET_DIR:~0,-1%"
)

rem 末尾 \ を除去
if "%TARGET_DIR:~-1%"=="\" set "TARGET_DIR=%TARGET_DIR:~0,-1%"

if "%TARGET_DIR%"=="" (
  echo [error] ディレクトリが空です。
  exit /b 2
)

if not exist "%TARGET_DIR%\" (
  echo [error] Directory not found: "%TARGET_DIR%"
  exit /b 2
)

echo Target: "%TARGET_DIR%"
echo.

rem ===== 2) カウント（dirで安定）=====
set /a TXT_COUNT=0
set /a NEEDTAG_COUNT=0

for /f %%A in ('dir /a:-d /s /b "%TARGET_DIR%\*.txt" 2^>nul ^| find /c /v ""') do set "TXT_COUNT=%%A"
for /f %%A in ('dir /a:-d /s /b "%TARGET_DIR%\*.needtag" 2^>nul ^| find /c /v ""') do set "NEEDTAG_COUNT=%%A"

echo .txt files to delete     : !TXT_COUNT!
echo .needtag files to delete : !NEEDTAG_COUNT!
echo.

if "!TXT_COUNT!"=="0" if "!NEEDTAG_COUNT!"=="0" (
  echo Nothing to delete.
  exit /b 0
)

set /p "CONFIRM=Type YES to proceed: "
if /I not "!CONFIRM!"=="YES" (
  echo Cancelled.
  exit /b 1
)

echo.
echo Deleting...

rem ===== 3) 削除（読み取り専用対策で attrib -r → del）=====
for /f "delims=" %%F in ('dir /a:-d /s /b "%TARGET_DIR%\*.txt" 2^>nul') do (
  attrib -r "%%F" 2>nul
  del /q "%%F" 2>nul
  if errorlevel 1 echo [warn] failed: "%%F"
)

for /f "delims=" %%F in ('dir /a:-d /s /b "%TARGET_DIR%\*.needtag" 2^>nul') do (
  attrib -r "%%F" 2>nul
  del /q "%%F" 2>nul
  if errorlevel 1 echo [warn] failed: "%%F"
)

echo Done.
echo ==== END %DATE% %TIME% ====
exit /b 0
