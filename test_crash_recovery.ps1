$ErrorActionPreference = "Stop"

$projectDir = Get-Location
$crashBuildDir = Join-Path $projectDir "build-crash-testing"
$tempDataDir = Join-Path $projectDir "temp_crash_server_data"

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

$env:PATH = "C:\msys64\ucrt64\bin;" + $env:PATH

$passedChecks = 0
$failedChecks = 0

function Report-Check($name, $condition) {
    if ($condition) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $script:passedChecks++
    } else {
        Write-Host "[FAIL] $name" -ForegroundColor Red
        $script:failedChecks++
    }
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Starting StreamForge Milestone 6 Crash Recovery Test Suite" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# -------------------------------------------------------------------------
# STEP 1: Build crash testing configuration in build-crash-testing
# -------------------------------------------------------------------------
Write-Host "`n>>> Configuring and building crash-testing binaries in $crashBuildDir..." -ForegroundColor Yellow
if (-not (Test-Path $crashBuildDir)) {
    New-Item -ItemType Directory -Path $crashBuildDir | Out-Null
}

& $cmake -S $projectDir -B $crashBuildDir -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER="C:/msys64/ucrt64/bin/g++.exe" -DCMAKE_MAKE_PROGRAM="C:/msys64/ucrt64/bin/mingw32-make.exe" -DSTREAMFORGE_CRASH_TESTING=ON | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "CMake configuration for crash testing failed!" -ForegroundColor Red
    exit 1
}

& $cmake --build $crashBuildDir --config Release | Out-Null
Report-Check "Build streamforge_crash_tests.exe (STREAMFORGE_CRASH_TESTING=ON)" ($LASTEXITCODE -eq 0)

# -------------------------------------------------------------------------
# STEP 2: Run streamforge_crash_tests.exe (all 4 simulated crash points)
# -------------------------------------------------------------------------
$crashTestsExe = Join-Path $crashBuildDir "streamforge_crash_tests.exe"
Write-Host "`n>>> Running simulated crash injection test binary ($crashTestsExe)..." -ForegroundColor Yellow
$crashOutput = & $crashTestsExe
$crashExit = $LASTEXITCODE

Write-Host $crashOutput
Report-Check "Crash injection: mid_write_record" ($crashOutput -match "Recovery verified cleanly for crash point 'mid_write_record'")
Report-Check "Crash injection: between_log_and_index" ($crashOutput -match "Recovery verified cleanly for crash point 'between_log_and_index'")
Report-Check "Crash injection: between_seal_and_sealed" ($crashOutput -match "Recovery verified cleanly for crash point 'between_seal_and_sealed'")
Report-Check "Crash injection: mid_write_offset_commit" ($crashOutput -match "Recovery verified cleanly for crash point 'mid_write_offset_commit'")
Report-Check "All simulated crash injection tests succeeded" ($crashExit -eq 0)

# -------------------------------------------------------------------------
# STEP 3: Real Broker Process Hard Kill (Stop-Process -Force) & Recovery
# -------------------------------------------------------------------------
Write-Host "`n>>> Running Real Server Hard-Kill & Restart Durability Test..." -ForegroundColor Yellow
$serverExe = Join-Path $crashBuildDir "streamforge_server.exe"
$cliExe = Join-Path $crashBuildDir "streamforge_cli.exe"
$testPort = 9295

if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

$serverProc = $null
try {
    # 1. Start Server
    # 1. Start Server
    $serverProc = Start-Process -FilePath $serverExe -ArgumentList "--port $testPort --data-dir `"$tempDataDir`" --log-level WARN" -PassThru -NoNewWindow
    
    $ready = $false
    $deadline = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $deadline) {
        $ping = & $cliExe --port $testPort ping 2>&1
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Milliseconds 100
    }
    Report-Check "Server started and responding to ping" $ready

    # 2. Create Topic & Produce 100 Records
    & $cliExe --port $testPort create-topic crash_topic 2 | Out-Null
    for ($i = 0; $i -lt 100; $i++) {
        & $cliExe --port $testPort produce crash_topic "payload_$i" --partition 0 | Out-Null
    }

    $outDesc = & $cliExe --port $testPort describe crash_topic
    Report-Check "Pre-kill topic state has 100 records" ($outDesc -match "next_offset=100")

    # 3. True Hard Kill (Stop-Process -Force)
    Write-Host "Hard killing server process (ID: $($serverProc.Id))..." -ForegroundColor Yellow
    Stop-Process -Id $serverProc.Id -Force
    Start-Sleep -Milliseconds 500
    $serverProc = $null

    # 4. Restart Server with same data dir
    Write-Host "Restarting server from existing data directory..." -ForegroundColor Yellow
    $serverProc = Start-Process -FilePath $serverExe -ArgumentList "--port $testPort --data-dir `"$tempDataDir`" --log-level WARN" -PassThru -NoNewWindow
    
    $ready = $false
    $deadline = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $deadline) {
        $ping = & $cliExe --port $testPort ping 2>&1
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Milliseconds 100
    }
    Report-Check "Server restarted and responding to ping" $ready

    # 5. Verify all acknowledged data is present
    $outDesc2 = & $cliExe --port $testPort describe crash_topic
    Report-Check "Post-kill recovered topic state has 100 records" ($outDesc2 -match "next_offset=100")

    $outFetch = & $cliExe --port $testPort fetch crash_topic 0 0 --max-messages 10
    Report-Check "Fetch from beginning succeeds after restart" ($outFetch -match "val=payload_0")

    # 6. Verify partition accepts new appends
    $outProd = & $cliExe --port $testPort produce crash_topic "after_crash_payload" --partition 0
    Report-Check "Produce new record after crash recovery succeeds" ($outProd -match "base_offset=100")

} finally {
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $tempDataDir) {
        Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "CRASH RECOVERY TEST SUMMARY" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Passed: $passedChecks" -ForegroundColor Green
Write-Host "Failed: $failedChecks" -ForegroundColor $(if ($failedChecks -eq 0) { "Green" } else { "Red" })

if ($failedChecks -gt 0) {
    exit 1
} else {
    exit 0
}
