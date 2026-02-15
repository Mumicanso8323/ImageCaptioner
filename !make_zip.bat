@echo off
setlocal

REM ===== 設定 =====
set ZIP_NAME=ImageCaptioner.zip

REM ===== git が使えるか確認 =====
git --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] git が見つかりません。
    exit /b 1
)

REM ===== git リポジトリ確認 =====
if not exist .git (
    echo [ERROR] このフォルダは git リポジトリではありません。
    exit /b 1
)

REM ===== zip 生成 =====
echo Creating %ZIP_NAME% ...
git archive --format=zip --output %ZIP_NAME% HEAD

if errorlevel 1 (
    echo [ERROR] zip の生成に失敗しました。
    exit /b 1
)

echo [OK] %ZIP_NAME% を生成しました。
