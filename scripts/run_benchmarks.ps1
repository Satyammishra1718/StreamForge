$ErrorActionPreference = "Continue"

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"
$serverExe = Join-Path $buildDir "streamforge_server.exe"
$cliExe = Join-Path $buildDir "streamforge_cli.exe"
$benchExe = Join-Path $buildDir "streamforge_bench.exe"
$storageExe = Join-Path $buildDir "streamforge_storage.exe"

# Locate CMake
$cmake = "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
if (-not (Test-Path $cmake)) {
    $cmake = Get-Command cmake -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source
}
$env:PATH = "C:\msys64\ucrt64\bin;" + $env:PATH

# Ensure main binaries exist
if (-not (Test-Path $benchExe) -or -not (Test-Path $serverExe)) {
    Write-Host "Binaries missing, running build.ps1..." -ForegroundColor Yellow
    & "$projectDir\build.ps1"
}

# Create benchmark results directory with timestamp
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$resultsDir = Join-Path $projectDir "benchmark_results\$timestamp"
New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null
$summaryLog = Join-Path $resultsDir "summary.txt"

function Log-Output($text) {
    Write-Host $text
    Add-Content -Path $summaryLog -Value $text
}

function Save-Scenario-Log($name, $content) {
    $f = Join-Path $resultsDir "$name.log"
    Set-Content -Path $f -Value $content
}

Log-Output "====================================================================="
Log-Output " StreamForge Master Performance Benchmark Suite"
Log-Output " Timestamp: $timestamp"
Log-Output "====================================================================="

# Display Host System Specs
$sysInfo = & $benchExe system-info
Log-Output $sysInfo

$port = 9091
$tempDataDir = Join-Path $projectDir "temp_bench_data"

function Clean-TempData() {
    if (Test-Path $tempDataDir) {
        Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 200
    }
}

function Start-Broker($args) {
    Clean-TempData
    $allArgs = "--port $port --data-dir `"$tempDataDir`" " + $args
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $serverExe
    $psi.Arguments = $allArgs
    $psi.UseShellExecute = $false
    $p = [System.Diagnostics.Process]::Start($psi)
    Start-Sleep -Milliseconds 700
    return $p
}

function Stop-Broker($p) {
    if ($p -and -not $p.HasExited) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        $p.WaitForExit(3000)
    }
    Start-Sleep -Milliseconds 300
}

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO A: PRODUCE THROUGHPUT (DURABILITY TRADEOFF: SYNC VS ASYNC)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO A: Produce Throughput (Sync vs Async Fsync Tradeoff)..."

# 1. Sync on append = true
$serverA1 = Start-Broker "--sync-on-append true --log-level WARN"
& $cliExe --port $port create-topic bench_sync 1 | Out-Null
$outSync = & $benchExe produce-throughput --port $port --topic bench_sync --record-size 100 --batch-size 50 --duration-sec 5 --warmup-sec 2 --label "Produce (sync_on_append=true)"
Stop-Broker $serverA1
Save-Scenario-Log "scenario_a_sync" $outSync
Log-Output ($outSync -join "`n")

# 2. Sync on append = false
$serverA2 = Start-Broker "--sync-on-append false --log-level WARN"
& $cliExe --port $port create-topic bench_async 1 | Out-Null
$outAsync = & $benchExe produce-throughput --port $port --topic bench_async --record-size 100 --batch-size 50 --duration-sec 5 --warmup-sec 2 --label "Produce (sync_on_append=false)"
Stop-Broker $serverA2
Save-Scenario-Log "scenario_a_async" $outAsync
Log-Output ($outAsync -join "`n")

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO B: PRODUCE SCALING (1, 2, 4, 8, 16 CONCURRENT CONNECTIONS)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO B: Produce Scaling (Multi-Connection Load against 16 Partitions)..."
$serverB = Start-Broker "--sync-on-append false --workers 8 --log-level WARN"
& $cliExe --port $port create-topic bench_scaling 16 | Out-Null

$scalingResults = @()
foreach ($conns in @(1, 2, 4, 8, 16)) {
    Log-Output "  Running with $conns concurrent producer connections..."
    $outScale = & $benchExe produce-scaling --port $port --topic bench_scaling --partitions 16 --record-size 100 --batch-size 50 --connections $conns --duration-sec 5 --warmup-sec 2 --label "Scaling (N=$conns)"
    Save-Scenario-Log "scenario_b_conns_$conns" $outScale
    Log-Output ($outScale -join "`n")
}
Stop-Broker $serverB

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO C: FETCH THROUGHPUT (BATCHING EFFECT: SMALL VS 1 MIB CLAMP)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO C: Fetch Throughput (Batching Effect)..."
$serverC = Start-Broker "--sync-on-append false --log-level WARN"
& $cliExe --port $port create-topic bench_fetch 1 | Out-Null
Log-Output "  Pre-populating topic with 50,000 records..."
& $cliExe --port $port produce-many bench_fetch 50000 --batch 200 --size 100 | Out-Null

Log-Output "  Testing Small Batch Fetch (4 KiB max_bytes, 20 messages)..."
$outFetchSmall = & $benchExe fetch-throughput --port $port --topic bench_fetch --max-bytes 4096 --max-messages 20 --duration-sec 5 --warmup-sec 2 --label "Fetch Small Batch (4 KiB)"
Save-Scenario-Log "scenario_c_fetch_small" $outFetchSmall
Log-Output ($outFetchSmall -join "`n")

Log-Output "  Testing Large Batch Fetch (1 MiB max_bytes, 500 messages)..."
$outFetchLarge = & $benchExe fetch-throughput --port $port --topic bench_fetch --max-bytes 1048500 --max-messages 500 --duration-sec 5 --warmup-sec 2 --label "Fetch Large Batch (1 MiB)"
Save-Scenario-Log "scenario_c_fetch_large" $outFetchLarge
Log-Output ($outFetchLarge -join "`n")
Stop-Broker $serverC

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO D: MIXED WORKLOAD (PRODUCERS + CONSUMERS + E2E LATENCY)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO D: Mixed Workload (4 Producers + 4 Consumers + End-to-End Latency)..."
$serverD = Start-Broker "--sync-on-append false --workers 8 --log-level WARN"
& $cliExe --port $port create-topic bench_mixed 4 | Out-Null
$outMixed = & $benchExe mixed-workload --port $port --topic bench_mixed --partitions 4 --producers 4 --consumers 4 --duration-sec 6 --warmup-sec 2
Save-Scenario-Log "scenario_d_mixed" $outMixed
Log-Output ($outMixed -join "`n")
Stop-Broker $serverD

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO E: CONNECTION SCALING (M4 EVENT LOOP: 10, 100, 500, 1000 CONNS)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO E: Connection Scaling (M4 Event Loop Thread Count & Ping Latency)..."
$serverE = Start-Broker "--max-connections 1200 --workers 4 --log-level WARN"
$connCounts = @(10, 100, 500, 1000)
foreach ($c in $connCounts) {
    Log-Output "  Testing with $c concurrent idle connections..."
    $outConn = & $benchExe connection-scaling --port $port --connections $c --duration-sec 4 --server-pid $serverE.Id
    Save-Scenario-Log "scenario_e_conn_$c" $outConn
    Log-Output ($outConn -join "`n")
}
Stop-Broker $serverE

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO F: BROKER RESTART RECOVERY TIME (QUICK VS FULL SCAN)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO F: Broker Restart Recovery Time (Quick vs Full Startup Scan)..."
$sizes = @(
    @{ Name = "1K"; Count = 1000 },
    @{ Name = "50K"; Count = 50000 },
    @{ Name = "100K"; Count = 100000 }
)

$recoveryTable = @()

foreach ($sz in $sizes) {
    $count = $sz.Count
    $label = $sz.Name
    Log-Output "  Populating dataset: $label records ($count records)..."
    Clean-TempData
    # Create topic first, then fill with 64KB segments
    & $storageExe --data-dir $tempDataDir create-topic "recov_topic" 1 | Out-Null
    $fillOut = & $storageExe --data-dir $tempDataDir --segment-bytes 65536 fill "recov_topic" $count 100
    
    # 1. Measure Quick Startup Scan
    $swQuick = [System.Diagnostics.Stopwatch]::StartNew()
    $psiQ = New-Object System.Diagnostics.ProcessStartInfo
    $psiQ.FileName = $serverExe
    $psiQ.Arguments = "--port $port --data-dir `"$tempDataDir`" --startup-scan quick --log-level WARN"
    $psiQ.UseShellExecute = $false
    $pQ = [System.Diagnostics.Process]::Start($psiQ)
    # Wait until ping responds
    $readyQ = $false
    while ($swQuick.ElapsedMilliseconds -lt 30000) {
        $png = & $cliExe --port $port ping 2>$null
        if ($png -match "PONG received") { $readyQ = $true; break }
        Start-Sleep -Milliseconds 10
    }
    $swQuick.Stop()
    $timeQuickMs = $swQuick.ElapsedMilliseconds
    Stop-Broker $pQ

    # 2. Measure Full Startup Scan
    $swFull = [System.Diagnostics.Stopwatch]::StartNew()
    $psiF = New-Object System.Diagnostics.ProcessStartInfo
    $psiF.FileName = $serverExe
    $psiF.Arguments = "--port $port --data-dir `"$tempDataDir`" --startup-scan full --log-level WARN"
    $psiF.UseShellExecute = $false
    $pF = [System.Diagnostics.Process]::Start($psiF)
    # Wait until ping responds
    $readyF = $false
    while ($swFull.ElapsedMilliseconds -lt 60000) {
        $png = & $cliExe --port $port ping 2>$null
        if ($png -match "PONG received") { $readyF = $true; break }
        Start-Sleep -Milliseconds 10
    }
    $swFull.Stop()
    $timeFullMs = $swFull.ElapsedMilliseconds
    Stop-Broker $pF

    $recoveryTable += [PSCustomObject]@{
        DatasetRecords = $label
        QuickScanMs = $timeQuickMs
        FullScanMs = $timeFullMs
        Speedup = if ($timeFullMs -gt 0) { [math]::Round($timeFullMs / [math]::Max(1.0, $timeQuickMs), 2) } else { 1.0 }
    }
}

Log-Output "`n---------------------------------------------------------------------"
Log-Output " RECOVERY TIME SUMMARY TABLE"
Log-Output "---------------------------------------------------------------------"
$recTableStr = $recoveryTable | Format-Table -AutoSize | Out-String
Log-Output $recTableStr
Save-Scenario-Log "scenario_f_recovery" $recTableStr

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO G: OLD-VS-NEW NETWORKING (M3 THREAD-PER-CONN VS M4 EVENT LOOP)
# ─────────────────────────────────────────────────────────────────────────────
Log-Output "`n>>> SCENARIO G: Old vs New Networking Architecture (M3 vs M4+)..."

$m3Dir = Join-Path $projectDir "temp_m3_tree"
$m3ServerExe = Join-Path $m3Dir "build\streamforge_server.exe"

# If worktree does not exist, add it
if (-not (Test-Path $m3Dir)) {
    Log-Output "  Creating git worktree for tag m3-thread-per-connection..."
    git worktree add temp_m3_tree m3-thread-per-connection | Out-Null
}

if (-not (Test-Path $m3ServerExe)) {
    Log-Output "  Building historical M3 server (thread-per-connection architecture)..."
    & $cmake -S $m3Dir -B "$m3Dir\build" -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER="C:/msys64/ucrt64/bin/g++.exe" -DCMAKE_MAKE_PROGRAM="C:/msys64/ucrt64/bin/mingw32-make.exe" | Out-Null
    & $cmake --build "$m3Dir\build" --target streamforge_server | Out-Null
}

$oldNewTable = @()

if (Test-Path $m3ServerExe) {
    Log-Output "  Comparing M3 Thread-Per-Connection vs M4 WSAPoll Event-Loop at 10, 50, 100, 200 connections..."
    $testConns = @(10, 50, 100, 200)

    foreach ($cn in $testConns) {
        # 1. Old M3 Server
        Clean-TempData
        $psiM3 = New-Object System.Diagnostics.ProcessStartInfo
        $psiM3.FileName = $m3ServerExe
        $psiM3.Arguments = "--port $port --data-dir `"$tempDataDir`" --log-level WARN"
        $psiM3.UseShellExecute = $false
        $pM3 = [System.Diagnostics.Process]::Start($psiM3)
        Start-Sleep -Milliseconds 600

        $outM3 = & $benchExe connection-scaling --port $port --connections $cn --duration-sec 3 --server-pid $pM3.Id
        $threadsM3 = "N/A"
        if (($outM3 -join "`n") -match "Server Thread Count:\s+(\d+)") { $threadsM3 = [int]$matches[1] }
        $p99M3 = "N/A"
        if (($outM3 -join "`n") -match "p99:\s+([\d\.]+)\s+ms") { $p99M3 = $matches[1] }
        Stop-Broker $pM3

        # 2. Modern M4+ Event-Loop Server
        $pM4 = Start-Broker "--workers 4 --log-level WARN"
        $outM4 = & $benchExe connection-scaling --port $port --connections $cn --duration-sec 3 --server-pid $pM4.Id
        $threadsM4 = "N/A"
        if (($outM4 -join "`n") -match "Server Thread Count:\s+(\d+)") { $threadsM4 = [int]$matches[1] }
        $p99M4 = "N/A"
        if (($outM4 -join "`n") -match "p99:\s+([\d\.]+)\s+ms") { $p99M4 = $matches[1] }
        Stop-Broker $pM4

        $oldNewTable += [PSCustomObject]@{
            Connections = $cn
            M3_Threads = $threadsM3
            M4_Threads = $threadsM4
            M3_p99_ms = $p99M3
            M4_p99_ms = $p99M4
        }
    }
}

Log-Output "`n---------------------------------------------------------------------"
Log-Output " OLD VS NEW NETWORKING COMPARISON TABLE"
Log-Output "---------------------------------------------------------------------"
$oldNewStr = $oldNewTable | Format-Table -AutoSize | Out-String
Log-Output $oldNewStr
Save-Scenario-Log "scenario_g_old_vs_new" $oldNewStr

# Cleanup temp files
Clean-TempData

Log-Output "`n====================================================================="
Log-Output " BENCHMARK SUITE COMPLETE"
Log-Output " Raw logs saved in: $resultsDir"
Log-Output "====================================================================="
