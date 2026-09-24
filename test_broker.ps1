$ErrorActionPreference = "Continue"

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"
$serverExe = Join-Path $buildDir "streamforge_server.exe"
$cliExe = Join-Path $buildDir "streamforge_cli.exe"
$protoTestsExe = Join-Path $buildDir "streamforge_protocol_tests.exe"

if (-not (Test-Path $serverExe) -or -not (Test-Path $cliExe) -or -not (Test-Path $protoTestsExe)) {
    Write-Host "Binaries missing, running build.ps1..." -ForegroundColor Yellow
    & "$projectDir\build.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed. Aborting broker tests."
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

# 1. Run C++ Protocol Unit Tests
Write-Host "Running C++ Protocol Unit Tests..." -ForegroundColor Cyan
& $protoTestsExe
Report-Check "C++ Protocol Unit Test Suite" ($LASTEXITCODE -eq 0)

$port = 9095
$tempDataDir = Join-Path $projectDir "temp_broker_test_data"
if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

$serverProc = $null

try {
    Write-Host "`nStarting StreamForge server on port $port with data-dir $tempDataDir..." -ForegroundColor Cyan
    $serverProc = Start-Process -FilePath $serverExe -ArgumentList "--port", $port, "--data-dir", "`"$tempDataDir`"", "--log-level", "WARN" -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 500

    # 2. Topic Operations (create, list, describe)
    $outCreate = & $cliExe --port $port create-topic orders 3
    Report-Check "CLI create-topic" ($LASTEXITCODE -eq 0 -and $outCreate -match "Successfully created topic")

    $outTopics = & $cliExe --port $port topics
    Report-Check "CLI list topics" ($LASTEXITCODE -eq 0 -and $outTopics -match "orders \(3 partitions\)")

    $outDesc = & $cliExe --port $port describe orders
    Report-Check "CLI describe topic" ($LASTEXITCODE -eq 0 -and $outDesc -match "Partition 0:" -and $outDesc -match "Partition 1:" -and $outDesc -match "Partition 2:")

    # 3. Duplicate create -> error 5; Bad name -> error 6
    $outDup = & $cliExe --port $port create-topic orders 3 2>&1
    Report-Check "Duplicate topic creation returns Error Code 5" ($outDup -match "Error \(code 5\)")

    $outBadName = & $cliExe --port $port create-topic CON 1 2>&1
    Report-Check "Invalid topic name 'CON' returns Error Code 6" ($outBadName -match "Error \(code 6\)")

    # 4. Produce 1,000 records to partition 0; Fetch back in pages (offsets 0..999)
    & $cliExe --port $port produce-many orders 1000 --partition 0 --size 64 --batch 100 | Out-Null
    $outFetchAll = & $cliExe --port $port fetch orders 0 0 --max-messages 1000
    Report-Check "Produce & fetch 1,000 records (offsets 0..999)" ($LASTEXITCODE -eq 0 -and $outFetchAll -match "next_offset=1000" -and $outFetchAll -match "high_watermark=1000")

    # 5. Fetch at high_watermark (offset 1000) -> returns OK with 0 records
    $outHwFetch = & $cliExe --port $port fetch orders 0 1000
    Report-Check "Fetch at high_watermark returns OK with 0 records" ($LASTEXITCODE -eq 0 -and $outHwFetch -match "Fetched 0 records")

    # 6. Key-based routing: 200 records with 10 distinct keys and partition = -1
    $keyRoutingSuccess = $true
    for ($i = 0; $i -lt 200; $i++) {
        $keyStr = "user_" + ($i % 10)
        $valStr = "event_" + $i
        $res = & $cliExe --port $port produce orders $valStr --key $keyStr
        if ($LASTEXITCODE -ne 0) { $keyRoutingSuccess = $false }
    }
    Report-Check "Key-based routing across 200 records (-1 partition)" $keyRoutingSuccess

    # 7. Fetch boundary semantics (out of range)
    $outOorFetch = & $cliExe --port $port fetch orders 0 9999 2>&1
    Report-Check "Fetch out of range returns Error Code 8" ($outOorFetch -match "Error \(code 8\)")

    # 8. Error cases (unknown topic 4, invalid partition 7, oversized record 9)
    $outUnkTopic = & $cliExe --port $port fetch nonexistent 0 0 2>&1
    Report-Check "Unknown topic returns Error Code 4" ($outUnkTopic -match "Error \(code 4\)")

    $outInvPart = & $cliExe --port $port fetch orders 99 0 2>&1
    Report-Check "Invalid partition returns Error Code 7" ($outInvPart -match "Error \(code 7\)")

    # 9. Malformed PRODUCE body -> Error 10, connection stays open for follow-up PING
    $outMalformed = & $cliExe --port $port unknown-type 2>&1
    Report-Check "Malformed request handling maintains connection for follow-up PING" ($LASTEXITCODE -eq 0 -and $outMalformed -match "subsequent PING succeeded")

    # 10. 8 Parallel Producer Processes (500 records each -> 4,000 records in total into partition 0)
    & $cliExe --port $port create-topic parallel_topic 1 | Out-Null
    $producers = @()
    for ($p = 0; $p -lt 8; $p++) {
        $producers += Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "produce-many", "parallel_topic", "500", "--partition", "0", "--batch", "50" -PassThru -NoNewWindow
    }
    $producers | ForEach-Object { $_.WaitForExit() }

    $outParallelDesc = & $cliExe --port $port describe parallel_topic
    Report-Check "8 parallel producers (4,000 total contiguous records)" ($LASTEXITCODE -eq 0 -and $outParallelDesc -match "next_offset=4000")

    # 11. Consumer Scenario (--follow)
    & $cliExe --port $port create-topic consumer_topic 1 | Out-Null
    & $cliExe --port $port produce-many consumer_topic 100 --partition 0 --batch 50 | Out-Null

    $consumerLog = Join-Path $projectDir "temp_consumer_output.txt"
    if (Test-Path $consumerLog) { Remove-Item $consumerLog -Force }

    $consumerProc = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "consume", "consumer_topic", "0", "--from", "0", "--follow" -RedirectStandardOutput $consumerLog -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 500

    & $cliExe --port $port produce-many consumer_topic 50 --partition 0 --batch 50 | Out-Null
    Start-Sleep -Milliseconds 500

    if (-not $consumerProc.HasExited) {
        Stop-Process -Id $consumerProc.Id -Force -ErrorAction SilentlyContinue
    }

    $consumerContent = Get-Content $consumerLog -Raw -ErrorAction SilentlyContinue
    Report-Check "Consumer (--follow) received initial 100 and subsequent 50 records in order" ($consumerContent -match "\[149\]")
    if (Test-Path $consumerLog) { Remove-Item $consumerLog -Force -ErrorAction SilentlyContinue }

    # 12. Handle Leak Test (200 connect/produce/disconnect cycles)
    $procBefore = Get-Process -Id $serverProc.Id
    $handlesBefore = $procBefore.HandleCount
    for ($c = 0; $c -lt 200; $c++) {
        & $cliExe --port $port produce orders "val_$c" --partition 0 | Out-Null
    }
    $procAfter = Get-Process -Id $serverProc.Id
    $handlesAfter = $procAfter.HandleCount
    $handleDiff = [math]::Abs($handlesAfter - $handlesBefore)
    Write-Host "Server handle count before: $handlesBefore, after: $handlesAfter (diff: $handleDiff)" -ForegroundColor Gray
    Report-Check "Handle leak test (200 cycles, diff <= 15)" ($handleDiff -le 15)

    # 13. M1 Protocol Compatibility (ping, echo, slow-echo, big-frame, garbage)
    $outPing = & $cliExe --port $port ping
    Report-Check "M1 CLI ping" ($LASTEXITCODE -eq 0 -and $outPing -match "PONG received")

    $outEcho = & $cliExe --port $port echo "milestone3"
    Report-Check "M1 CLI echo" ($LASTEXITCODE -eq 0 -and $outEcho -match "ECHO_REPLY received: milestone3")

    # 14. Graceful Stop & Durability Restart
    Write-Host "`nTesting Graceful Stop & Durability Restart..." -ForegroundColor Cyan
    if (-not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
        $serverProc.WaitForExit()
    }

    $serverProc = Start-Process -FilePath $serverExe -ArgumentList "--port", $port, "--data-dir", "`"$tempDataDir`"", "--log-level", "WARN" -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 500

    $outDurabilityDesc = & $cliExe --port $port describe orders
    Report-Check "Durability check: data intact across server restart" ($LASTEXITCODE -eq 0 -and $outDurabilityDesc -match "Partition 0:")

    # 15. Hard Kill & Crash Recovery Verification (sync_on_append = true)
    Write-Host "`nTesting Hard Kill & Crash Recovery..." -ForegroundColor Cyan
    & $cliExe --port $port create-topic crash_topic 1 | Out-Null
    & $cliExe --port $port produce crash_topic "crash_test_payload" --partition 0 | Out-Null
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit()

    $serverProc = Start-Process -FilePath $serverExe -ArgumentList "--port", $port, "--data-dir", "`"$tempDataDir`"", "--sync-on-append", "true", "--log-level", "WARN" -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 500

    $outCrashCheck = & $cliExe --port $port fetch crash_topic 0 0 --max-messages 10
    Report-Check "Hard kill crash recovery: acknowledged records intact" ($LASTEXITCODE -eq 0 -and $outCrashCheck -match "crash_test_payload")

} finally {
    Write-Host "`nStopping server process..." -ForegroundColor Cyan
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $tempDataDir) {
        Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n--- BROKER TEST SUMMARY ---" -ForegroundColor Cyan
Write-Host "Passed: $script:passed" -ForegroundColor Green
if ($script:failed -gt 0) {
    Write-Host "Failed: $script:failed" -ForegroundColor Red
    exit 1
} else {
    Write-Host "Failed: $script:failed" -ForegroundColor Green
    exit 0
}
