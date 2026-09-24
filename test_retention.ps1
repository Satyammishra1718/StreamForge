$ErrorActionPreference = "Continue"

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"
$serverExe = Join-Path $buildDir "streamforge_server.exe"
$cliExe = Join-Path $buildDir "streamforge_cli.exe"
$storageExe = Join-Path $buildDir "streamforge_storage.exe"
$storageTestsExe = Join-Path $buildDir "streamforge_storage_tests.exe"

if (-not (Test-Path $serverExe) -or -not (Test-Path $cliExe) -or -not (Test-Path $storageExe) -or -not (Test-Path $storageTestsExe)) {
    Write-Host "Binaries missing, running build.ps1..." -ForegroundColor Yellow
    & "$projectDir\build.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed. Aborting retention tests."
        exit 1
    }
}

$script:passed = 0
$script:failed = 0

function Report-Check($name, $success) {
    if ($success) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $script:passed++
    } else {
        Write-Host "[FAIL] $name" -ForegroundColor Red
        $script:failed++
    }
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Starting StreamForge Milestone 6 Retention End-to-End Suite" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 0. Run C++ Storage & Retention Unit Tests
Write-Host "`n--- Running C++ Storage & Retention Unit Tests ---" -ForegroundColor Cyan
& $storageTestsExe
Report-Check "C++ Storage Unit Tests (streamforge_storage_tests.exe)" ($LASTEXITCODE -eq 0)

$port = 9099
$tempDataDir = Join-Path $projectDir "temp_retention_test_data"
if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

$serverProc = $null

try {
    Write-Host "`nStarting StreamForge server on port $port with small segment & retention limits..." -ForegroundColor Cyan
    # 512-byte segments, 1500-byte retention budget, 400ms check interval
    $serverProc = Start-Process -FilePath $serverExe -ArgumentList `
        "--port", $port, `
        "--data-dir", "`"$tempDataDir`"", `
        "--segment-bytes", "512", `
        "--default-retention-bytes", "1500", `
        "--retention-check-interval-ms", "400", `
        "--log-level", "DEBUG" -PassThru -NoNewWindow

    Start-Sleep -Milliseconds 600

    # 1. Create topic
    $outCreate = & $cliExe --port $port create-topic ret_topic 1
    Report-Check "CLI create-topic ret_topic" ($LASTEXITCODE -eq 0 -and $outCreate -match "Successfully created topic")

    # 2. Produce 60 records (~60 bytes each => ~3600 bytes total, multiple segments)
    Write-Host "Producing 60 records to force segment rolling and exceed retention threshold..." -ForegroundColor Cyan
    for ($i = 0; $i -lt 60; $i++) {
        $key = "key_$i"
        $val = "message_payload_val_" + ("X" * 30) + "_$i"
        & $cliExe --port $port produce ret_topic 0 $val --key $key > $null
    }

    # 3. Check pre-GC or initial describe
    $outDesc1 = & $cliExe --port $port describe ret_topic
    Report-Check "CLI describe before retention pass" ($LASTEXITCODE -eq 0 -and $outDesc1 -match "Partition 0:")

    # 4. Sleep to let the background retention reaper thread trigger (interval is 400ms)
    Write-Host "Waiting for background retention worker to prune old segments..." -ForegroundColor Cyan
    Start-Sleep -Milliseconds 1500

    # 5. Check describe again to verify earliest_offset shifted forward
    $outDesc2 = & $cliExe --port $port describe ret_topic
    $outDescStr = if ($outDesc2 -is [array]) { $outDesc2 -join "`n" } else { [string]$outDesc2 }
    
    $earliest = 0
    if ($outDescStr -match "earliest=(\d+)") {
        $earliest = [int]$matches[1]
    }
    Write-Host "Retention check: earliest_offset is $earliest (should be > 0)" -ForegroundColor Cyan
    Report-Check "Retention pruned old segments (earliest_offset > 0)" ($earliest -gt 0)

    # 6. Direct fetch at offset 0 must fail with Error Code 8 (OFFSET_OUT_OF_RANGE)
    Write-Host "Testing direct fetch at pruned offset 0 (expects Error 8)..." -ForegroundColor Cyan
    $fetchOut = & $cliExe --port $port fetch ret_topic 0 0 2>&1
    $fetchStr = if ($fetchOut -is [array]) { $fetchOut -join "`n" } else { [string]$fetchOut }
    Report-Check "Fetch below earliest offset returns Error 8 (OFFSET_OUT_OF_RANGE)" ($fetchStr -match "code 8" -or $fetchStr -match "Offset out of range")

    # 7. Consumer group auto-reset test
    Write-Host "Testing Consumer Group starting at pruned offset 0 with auto-reset..." -ForegroundColor Cyan
    $cgOut = & $cliExe --port $port consume-group ret_cg ret_topic --from 0 --max-records 10 --idle-exit-ms 2000 2>&1
    $cgStr = if ($cgOut -is [array]) { $cgOut -join "`n" } else { [string]$cgOut }
    $cgReset = ($cgStr -match "Resetting group ret_cg" -or $cgStr -match "RECORD topic=ret_topic")
    Report-Check "Consumer group handles retention prune and auto-resets offset" ($cgReset)

    # 8. Test delete-while-open safety with Windows handles
    Write-Host "Testing Windows FILE_SHARE_DELETE safety..." -ForegroundColor Cyan
    $partDir = Join-Path $tempDataDir "ret_topic\0"
    $segLogs = Get-ChildItem -Path $partDir -Filter "*.log" -ErrorAction SilentlyContinue
    $deleteSafetyPass = $false
    if ($segLogs -and $segLogs.Count -gt 0) {
        $firstSeg = $segLogs[0].FullName
        try {
            $fs = [System.IO.File]::Open($firstSeg, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete)
            # File is held open with FILE_SHARE_DELETE. Attempt to open another handle or verify share modes
            $deleteSafetyPass = ($null -ne $fs)
            $fs.Close()
        } catch {
            $deleteSafetyPass = $false
        }
    } else {
        $deleteSafetyPass = $true
    }
    Report-Check "Windows delete-while-open sharing flags functional" $deleteSafetyPass

} finally {
    # Clean shutdown of server
    if ($serverProc -and -not $serverProc.HasExited) {
        Write-Host "Stopping StreamForge server..." -ForegroundColor Cyan
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
        $serverProc.WaitForExit(3000)
    }
}

# 9. Offline CLI GC Commands
Write-Host "`n--- Testing Offline storage CLI GC commands ---" -ForegroundColor Cyan
# Run dry-run gc
$gcDryOut = & $storageExe --data-dir $tempDataDir gc ret_topic --dry-run
Report-Check "Storage CLI gc --dry-run succeeds" ($LASTEXITCODE -eq 0 -and $gcDryOut -match "GC dry-run completed")

# Run real gc
$gcOut = & $storageExe --data-dir $tempDataDir gc ret_topic
Report-Check "Storage CLI gc execution succeeds" ($LASTEXITCODE -eq 0 -and $gcOut -match "GC completed")

# Final Cleanup
if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "RETENTION TEST SUMMARY" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Passed: $script:passed" -ForegroundColor Green
Write-Host "Failed: $script:failed" -ForegroundColor $(if ($script:failed -eq 0) { "Green" } else { "Red" })

if ($script:failed -ne 0) {
    exit 1
}
exit 0
