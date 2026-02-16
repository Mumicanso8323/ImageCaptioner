@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem =========================================================
rem  即閉じ対策：自分を cmd /k で再起動してウィンドウを保持する
rem  ログ：同じフォルダに clean_txt_last.log
rem =========================================================

rem --- "内側起動" でなければ cmd /k で自分を call して再起動 ---
if /I not "%~1"=="--inner" (
  set "SELF=%~f0"
  set "LOG=%~dp0clean_txt_last.log"
  (echo ==== START %DATE% %TIME% ====)> "%LOG%"

  rem ★ここが重要：cmd /k の中で call "%SELF%" する（クォート崩れ防止）
  start "clean_txt" cmd /k call "%SELF%" --inner %*
  exit /b
)

shift

rem --- ここから本処理（ログへリダイレクト） ---
call :main %* 1>>"%~dp0clean_txt_last.log" 2>>&1
echo.
echo (log) "%~dp0clean_txt_last.log"
echo.
pause
exit /b

:main
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
exit /b 0
