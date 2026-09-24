$ErrorActionPreference = "Continue"

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"
$serverExe = Join-Path $buildDir "streamforge_server.exe"
$cliExe = Join-Path $buildDir "streamforge_cli.exe"

if (-not (Test-Path $serverExe) -or -not (Test-Path $cliExe)) {
    Write-Host "Binaries missing, running build.ps1..." -ForegroundColor Yellow
    & "$projectDir\build.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed. Aborting tests."
        exit 1
    }
}

$testPort = 9093
$passed = 0
$failed = 0

function Report-Check($name, $success) {
    if ($success) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $global:passed++
    } else {
        Write-Host "[FAIL] $name" -ForegroundColor Red
        $global:failed++
    }
}

Write-Host "Starting background StreamForge server on port $testPort..." -ForegroundColor Cyan
$serverProc = Start-Process -FilePath $serverExe -ArgumentList "$testPort" -PassThru -WindowStyle Hidden
Start-Sleep -Milliseconds 600

try {
    # 1. PING
    $out = & $cliExe --port $testPort ping
    Report-Check "CLI ping" ($LASTEXITCODE -eq 0 -and $out -match "PONG")

    # 2. ECHO
    $out = & $cliExe --port $testPort echo "hello world"
    Report-Check "CLI echo" ($LASTEXITCODE -eq 0 -and $out -match "hello world")

    # 3. SLOW-ECHO
    $out = & $cliExe --port $testPort slow-echo "hello"
    Report-Check "CLI slow-echo" ($LASTEXITCODE -eq 0 -and $out -match "hello")

    # 4. BIG-FRAME
    $out = & $cliExe --port $testPort big-frame
    Report-Check "CLI big-frame (code 2 rejected & closed)" ($LASTEXITCODE -eq 0 -and $out -match "code 2")

    # 5. GARBAGE
    $out = & $cliExe --port $testPort garbage
    Report-Check "CLI garbage (code 3 rejected & closed)" ($LASTEXITCODE -eq 0 -and $out -match "code 3")

    # 6. UNKNOWN-TYPE
    $out = & $cliExe --port $testPort unknown-type
    Report-Check "CLI unknown-type (code 1 & open socket PING succeeded)" ($LASTEXITCODE -eq 0 -and $out -match "code 1")

    # 7. PARALLEL CLIENTS (5 parallel clients)
    Write-Host "Testing 5 parallel clients..." -ForegroundColor Cyan
    $jobs = @()
    for ($i = 1; $i -le 5; $i++) {
        $jobs += Start-Job -ScriptBlock {
            param($cli, $port, $id)
            & $cli --port $port echo "parallel-client-$id"
        } -ArgumentList $cliExe, $testPort, $i
    }

    $parallelSuccess = $true
    foreach ($job in $jobs) {
        $jOut = Receive-Job -Job $job -Wait
        if ($jOut -notmatch "parallel-client-") {
            $parallelSuccess = $false
        }
        Remove-Job -Job $job
    }
    Report-Check "5 parallel CLI clients" $parallelSuccess

    # 8. ABRUPT DISCONNECT TEST
    Write-Host "Testing abrupt client kill during slow-echo..." -ForegroundColor Cyan
    $slowJobProc = Start-Process -FilePath $cliExe -ArgumentList "--port $testPort slow-echo 'test-abrupt-kill'" -PassThru -WindowStyle Hidden
    Start-Sleep -Milliseconds 50
    Stop-Process -Id $slowJobProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 200
    # Verify server is still alive after abrupt client kill
    $out = & $cliExe --port $testPort ping
    Report-Check "Abrupt client kill resilience" ($LASTEXITCODE -eq 0 -and $out -match "PONG")

    # 9. HANDLE LEAK CHECK (200 connect/disconnect cycles)
    Write-Host "Testing handle leaks (200 connect/disconnect cycles)..." -ForegroundColor Cyan
    $proc = Get-Process -Id $serverProc.Id
    $handlesBefore = $proc.HandleCount

    for ($i = 0; $i -lt 200; $i++) {
        $null = & $cliExe --port $testPort ping
    }

    Start-Sleep -Milliseconds 300
    $proc.Refresh()
    $handlesAfter = $proc.HandleCount
    $handleDiff = [math]::Abs($handlesAfter - $handlesBefore)
    Write-Host "Handles before: $handlesBefore, after: $handlesAfter (diff: $handleDiff)" -ForegroundColor Gray
    Report-Check "Handle leak test (200 cycles, diff <= 10)" ($handleDiff -le 10)

} finally {
    Write-Host "Stopping server process..." -ForegroundColor Cyan
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n--- TEST SUMMARY ---" -ForegroundColor Cyan
Write-Host "Passed: $passed" -ForegroundColor Green
if ($failed -gt 0) {
    Write-Host "Failed: $failed" -ForegroundColor Red
    exit 1
} else {
    Write-Host "Failed: $failed" -ForegroundColor Green
    exit 0
}
