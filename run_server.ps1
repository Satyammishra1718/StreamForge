$ErrorActionPreference = "Stop"

$port = if ($args.Count -ge 1) { $args[0] } else { "9092" }

$serverExe = Join-Path (Get-Location) "build\streamforge_server.exe"
if (-not (Test-Path $serverExe)) {
    Write-Host "Server executable not found. Running build.ps1..." -ForegroundColor Yellow
    .\build.ps1
}

Write-Host "Starting StreamForge server on port $port..." -ForegroundColor Cyan
& $serverExe $port
