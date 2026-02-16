@echo off
setlocal

set TARGET=D:\ai\kohya_ss\dataset\images 2\1_shigure_ui

echo ===============================
echo  TXT / NEEDTAG 一括削除
echo ===============================
echo.
echo 対象フォルダ:
echo "%TARGET%"
echo.

if not exist "%TARGET%" (
    echo フォルダが存在しません。
    pause
    exit /b
)

echo 削除中...

del /s /q "%TARGET%\*.txt" 2>nul
del /s /q "%TARGET%\*.needtag" 2>nul

echo.
echo 完了しました。
