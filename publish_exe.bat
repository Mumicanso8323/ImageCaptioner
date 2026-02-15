@echo off
setlocal

REM Build single-file exe (win-x64)
dotnet publish ImageCaptioner.csproj -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true /p:EnableCompressionInSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true

if errorlevel 1 (
  echo [ERROR] publish failed
  exit /b 1
)

echo [OK] EXE: bin\Release\net8.0\win-x64\publish\ImageCaptioner.exe
