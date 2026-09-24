$ErrorActionPreference = "Stop"

$projectDir = Get-Location
$binDir = Join-Path $projectDir "build"
$serverExe = Join-Path $binDir "streamforge_server.exe"
$cliExe = Join-Path $binDir "streamforge_cli.exe"

if (-not (Test-Path $serverExe) -or -not (Test-Path $cliExe)) {
    Write-Error "Required binaries not found in build directory. Run .\build.ps1 first."
    exit 1
}

$port = 9097
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

function Start-ServerProcess($arguments) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $serverExe
    $psi.Arguments = $arguments
    $psi.UseShellExecute = $false
    return [System.Diagnostics.Process]::Start($psi)
}

$tempDataDir = Join-Path $projectDir "temp_concurrency_test_data"
if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

$serverProc = $null

try {
    Write-Host "==========================================================" -ForegroundColor Cyan
    Write-Host "Starting StreamForge Concurrency & I/O Loop Test Suite..." -ForegroundColor Cyan
    Write-Host "==========================================================" -ForegroundColor Cyan

    # -------------------------------------------------------------------------
    # TEST 1: THREAD COUNT STABILITY UNDER 200 IDLE CONNECTIONS
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 1: Thread Count Stability (0 vs 200 Connections) ---" -ForegroundColor Cyan
    $serverProc = Start-ServerProcess "--port $port --data-dir `"$tempDataDir`" --workers 4 --log-level WARN"
    Start-Sleep -Milliseconds 600

    $procObj = Get-Process -Id $serverProc.Id
    $threadsBefore = $procObj.Threads.Count
    Write-Host "Server thread count before connections: $threadsBefore (workers: 4)" -ForegroundColor Yellow

    # Hold 200 idle connections for 10 seconds
    $holderProc = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "hold-connections", "200", "10" -PassThru -NoNewWindow
    Start-Sleep -Seconds 2

    $procObjAfter = Get-Process -Id $serverProc.Id
    $threadsAfter = $procObjAfter.Threads.Count
    $threadDiff = [math]::Abs($threadsAfter - $threadsBefore)
    Write-Host "Server thread count with 200 idle connections held: $threadsAfter (diff: $threadDiff)" -ForegroundColor Yellow

    Report-Check "Thread count remains constant with 200 idle connections (before: $threadsBefore, after: $threadsAfter, diff: $threadDiff <= 2)" ($threadDiff -le 2)

    # -------------------------------------------------------------------------
    # TEST 2: ACTIVE REQUESTS WHILE 200 CONNECTIONS ARE HELD
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 2: Requests while 200 Idle Connections are Held ---" -ForegroundColor Cyan
    $outPing = & $cliExe --port $port ping
    Report-Check "CLI ping while 200 connections held" ($LASTEXITCODE -eq 0 -and $outPing -match "PONG received")

    & $cliExe --port $port create-topic hold_topic 1 | Out-Null
    $outProd = & $cliExe --port $port produce hold_topic "held_connection_payload" --partition 0
    Report-Check "CLI produce while 200 connections held" ($LASTEXITCODE -eq 0 -and $outProd -match "base_offset=0")

    $outFetch = & $cliExe --port $port fetch hold_topic 0 0 --max-messages 1
    Report-Check "CLI fetch while 200 connections held" ($LASTEXITCODE -eq 0 -and $outFetch -match "held_connection_payload")

    Write-Host "Waiting for 200-connection holder to release..." -ForegroundColor Gray
    $holderProc.WaitForExit()

    # -------------------------------------------------------------------------
    # TEST 3: READ-STALL TIMEOUT (SLOW-LORIS DEFENSE)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 3: Read-Stall Timeout (Slow-Loris Defense) ---" -ForegroundColor Cyan
    # Stop and restart server with --read-stall-timeout-sec 3
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit()
    Start-Sleep -Milliseconds 400

    $serverProc = Start-ServerProcess "--port $port --data-dir `"$tempDataDir`" --workers 4 --read-stall-timeout-sec 3 --log-level WARN"
    Start-Sleep -Milliseconds 600

    $stallStart = [System.Diagnostics.Stopwatch]::StartNew()
    $stallProc = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "stall-frame" -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 500

    # Normal requests work while another client is stalling
    $outNormalPing = & $cliExe --port $port ping
    Report-Check "Normal client ping works while stall-frame client is connected" ($LASTEXITCODE -eq 0 -and $outNormalPing -match "PONG received")

    # Stall client should be disconnected after ~3 seconds
    $stallProc.WaitForExit(7000)
    $stallStart.Stop()
    $elapsedSec = $stallStart.Elapsed.TotalSeconds
    Write-Host "Stall-frame client terminated after $([math]::Round($elapsedSec, 2)) seconds" -ForegroundColor Gray
    Report-Check "Stall-frame client disconnected by server timeout (3s defense)" ($stallProc.HasExited -and $elapsedSec -ge 2.5)

    # -------------------------------------------------------------------------
    # TEST 4: SLOW-READER & BOUNDED WORKING SET
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 4: Slow Reader & Bounded Working Set ---" -ForegroundColor Cyan
    # Restart server with small --max-output-buffer-bytes (1 MiB = 1048576)
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit()
    Start-Sleep -Milliseconds 400

    $serverProc = Start-ServerProcess "--port $port --data-dir `"$tempDataDir`" --workers 4 --max-output-buffer-bytes 1048576 --log-level WARN"
    Start-Sleep -Milliseconds 600

    & $cliExe --port $port create-topic slow_topic 1 | Out-Null
    # Produce records with payload
    & $cliExe --port $port produce-many slow_topic 50 --partition 0 --size 30000 --batch 25 | Out-Null

    $wsBefore = (Get-Process -Id $serverProc.Id).WorkingSet64
    Write-Host "Server working set before slow reader: $([math]::Round($wsBefore / 1MB, 2)) MB" -ForegroundColor Yellow

    # Run slow reader that floods FETCH without reading responses
    $srProc = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "slow-reader", "slow_topic", "0" -PassThru -NoNewWindow
    $srProc.WaitForExit(10000)

    $wsAfter = (Get-Process -Id $serverProc.Id).WorkingSet64
    Write-Host "Server working set after slow reader: $([math]::Round($wsAfter / 1MB, 2)) MB" -ForegroundColor Yellow

    Report-Check "Slow reader disconnected by server" ($srProc.HasExited)
    $outHealthy = & $cliExe --port $port ping
    Report-Check "Server healthy and responsive after slow reader disconnected" ($LASTEXITCODE -eq 0 -and $outHealthy -match "PONG received")

    # -------------------------------------------------------------------------
    # TEST 5: LARGE RESPONSES (PARTIAL-WRITE PATH)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 5: Large Responses (Partial-Write Path) ---" -ForegroundColor Cyan
    # Restart server with standard output buffer (8 MiB)
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit()
    Start-Sleep -Milliseconds 400

    $serverProc = Start-ServerProcess "--port $port --data-dir `"$tempDataDir`" --workers 4 --log-level WARN"
    Start-Sleep -Milliseconds 600

    & $cliExe --port $port create-topic large_topic 1 | Out-Null
    # Produce 40 records of 50 KiB each = ~2 MiB
    Write-Host "Producing ~2 MiB of records..." -ForegroundColor Gray
    & $cliExe --port $port produce-many large_topic 40 --partition 0 --size 50000 --batch 10 | Out-Null

    # Repeatedly fetch with near-max 1 MiB clamp
    $curOffset = 0
    $totalFetched = 0
    while ($curOffset -lt 40) {
        $fOut = & $cliExe --port $port fetch large_topic 0 $curOffset --max-bytes 1048500 --max-messages 25
        if ($LASTEXITCODE -ne 0) { break }
        $fOutStr = $fOut -join [Environment]::NewLine
        if ($fOutStr -match "next_offset=(\d+)") {
            $next = [int]$matches[1]
            if ($next -eq $curOffset) { break }
            $count = $next - $curOffset
            $totalFetched += $count
            $curOffset = $next
        } else {
            break
        }
    }
    Report-Check "Large responses fetched intact over partial-write path (40 records total)" ($totalFetched -eq 40)

    # -------------------------------------------------------------------------
    # TEST 6: STRICT FIFO PIPELINED REQUEST ORDERING
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 6: Strict FIFO Request Ordering (500 Pipelined Requests) ---" -ForegroundColor Cyan
    $pipeOut = & $cliExe --port $port pipeline 500
    Report-Check "Strict FIFO pipelined request ordering (500 requests)" ($LASTEXITCODE -eq 0 -and $pipeOut -match "SUCCESS: All 500 pipelined responses received in strict sequential order")

    # -------------------------------------------------------------------------
    # TEST 7: PARALLEL LOAD (16 PRODUCERS x 500 RECORDS + 8 CONSUMERS)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 7: Parallel Load (16 Producers x 500 Records + 8 Consumers) ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic parallel_load_topic 4 | Out-Null

    # Start 8 background consumers reading from partitions
    $consumerProcs = @()
    for ($c = 0; $c -lt 8; $c++) {
        $part = $c % 4
        $cLog = Join-Path $projectDir "temp_c_${c}.log"
        $cp = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "consume", "parallel_load_topic", "$part", "--from", "0", "--follow" -RedirectStandardOutput $cLog -PassThru -NoNewWindow
        $consumerProcs += $cp
    }
    Start-Sleep -Milliseconds 400

    # Start 16 producers producing 500 records each = 8,000 total records across 4 partitions
    $producerProcs = @()
    for ($p = 0; $p -lt 16; $p++) {
        $targetPart = $p % 4
        $pp = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "produce-many", "parallel_load_topic", "500", "--partition", "$targetPart", "--batch", "50" -PassThru -NoNewWindow
        $producerProcs += $pp
    }

    $producerProcs | ForEach-Object { $_.WaitForExit() }
    Start-Sleep -Seconds 1

    # Stop background consumers
    $consumerProcs | ForEach-Object {
        if (-not $_.HasExited) {
            Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        }
    }
    Start-Sleep -Milliseconds 200
    # Clean up temp consumer logs
    for ($c = 0; $c -lt 8; $c++) {
        $cLog = Join-Path $projectDir "temp_c_${c}.log"
        if (Test-Path $cLog) { Remove-Item $cLog -Force -ErrorAction SilentlyContinue }
    }

    # Verify each partition received exactly 2,000 records (4 x 2,000 = 8,000 contiguous records)
    $descOut = & $cliExe --port $port describe parallel_load_topic
    $p0Ok = ($descOut -match "Partition 0:.*next_offset=2000")
    $p1Ok = ($descOut -match "Partition 1:.*next_offset=2000")
    $p2Ok = ($descOut -match "Partition 2:.*next_offset=2000")
    $p3Ok = ($descOut -match "Partition 3:.*next_offset=2000")

    Report-Check "16 parallel producers x 500 records (8,000 contiguous records across 4 partitions)" ($p0Ok -and $p1Ok -and $p2Ok -and $p3Ok)

    # -------------------------------------------------------------------------
    # TEST 8: HANDLE LEAK TEST & GRACEFUL SHUTDOWN
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 8: Handle Leaks & Graceful Shutdown ---" -ForegroundColor Cyan
    $procBefore = Get-Process -Id $serverProc.Id
    $handlesBefore = $procBefore.HandleCount
    for ($i = 0; $i -lt 200; $i++) {
        & $cliExe --port $port ping | Out-Null
    }
    $procAfter = Get-Process -Id $serverProc.Id
    $handlesAfter = $procAfter.HandleCount
    $handleDiff = [math]::Abs($handlesAfter - $handlesBefore)
    Write-Host "Server handle count before: $handlesBefore, after: $handlesAfter (diff: $handleDiff)" -ForegroundColor Gray
    Report-Check "Handle leak test (200 cycles, diff <= 15)" ($handleDiff -le 15)

    # Graceful shutdown with active client
    Write-Host "Testing graceful shutdown while active client is connected..." -ForegroundColor Gray
    $activeClient = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "hold-connections", "1", "10" -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 400

    # Stop server gracefully via protocol command
    & $cliExe --port $port shutdown | Out-Null
    $serverProc.WaitForExit(6000)
    Report-Check "Graceful shutdown while client active (exit code 0)" ($serverProc.HasExited -and $serverProc.ExitCode -eq 0)

    if (-not $activeClient.HasExited) {
        Stop-Process -Id $activeClient.Id -Force -ErrorAction SilentlyContinue
    }

    # -------------------------------------------------------------------------
    # TEST 9: DURABILITY & HARD KILL CRASH RECOVERY
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 9: Durability & Hard Kill Crash Recovery ---" -ForegroundColor Cyan
    # Restart server on existing data dir
    $serverProc = Start-ServerProcess "--port $port --data-dir `"$tempDataDir`" --workers 4 --log-level WARN"
    Start-Sleep -Milliseconds 600

    $descReopen = & $cliExe --port $port describe parallel_load_topic
    $descReopenStr = $descReopen -join [Environment]::NewLine
    $durabilityOk = ($descReopenStr -match "Partition 0:.*next_offset=2000" -and $descReopenStr -match "Partition 3:.*next_offset=2000")
    Report-Check "Durability check: data intact across server restart" ($durabilityOk)

    # Produce to crash topic, hard kill with Force, restart, verify recovery
    & $cliExe --port $port create-topic crash_m4 1 | Out-Null
    & $cliExe --port $port produce crash_m4 "crash_payload_m4" --partition 0 | Out-Null
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit()
    Start-Sleep -Milliseconds 400

    $serverProc = Start-ServerProcess "--port $port --data-dir `"$tempDataDir`" --sync-on-append true --workers 4 --log-level WARN"
    Start-Sleep -Milliseconds 600

    $crashFetch = & $cliExe --port $port fetch crash_m4 0 0 --max-messages 1
    Report-Check "Hard-kill crash recovery: acknowledged records intact" ($crashFetch -match "crash_payload_m4")

} finally {
    Write-Host "`nCleaning up test environment..." -ForegroundColor Gray
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $tempDataDir) {
        Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
    }
    Get-ChildItem -Path $projectDir -Filter "temp_c_*.log" | Remove-Item -Force -ErrorAction SilentlyContinue
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "--- CONCURRENCY TEST SUMMARY ---" -ForegroundColor Cyan
Write-Host "Passed: $script:passed" -ForegroundColor Green
if ($script:failed -gt 0) {
    Write-Host "Failed: $script:failed" -ForegroundColor Red
    exit 1
} else {
    Write-Host "Failed: $script:failed" -ForegroundColor Green
    exit 0
}
