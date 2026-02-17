@echo off
setlocal EnableExtensions

pushd "%~dp0" >nul 2>&1
if errorlevel 1 exit /b 1

call "%~dp0tool.bat" publish
set "EXIT_CODE=%ERRORLEVEL%"

popd
exit /b %EXIT_CODE%
