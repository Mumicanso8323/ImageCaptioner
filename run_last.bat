@echo off
setlocal

set "EXE=ImageCaptioner.exe"

if "%~1"=="" (
  echo Running: %EXE% --use-last
  %EXE% --use-last
) else (
  echo Running: %EXE% --use-last --dir "%~1"
  %EXE% --use-last --dir "%~1"
)

set "EXIT_CODE=%ERRORLEVEL%"
exit /b %EXIT_CODE%
