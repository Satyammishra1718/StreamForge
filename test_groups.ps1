$ErrorActionPreference = "Continue"

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"
$serverExe = Join-Path $buildDir "streamforge_server.exe"
$cliExe = Join-Path $buildDir "streamforge_cli.exe"
$groupTestsExe = Join-Path $buildDir "streamforge_group_tests.exe"
$protoTestsExe = Join-Path $buildDir "streamforge_protocol_tests.exe"

if (-not (Test-Path $serverExe) -or -not (Test-Path $cliExe)) {
    Write-Host "Binaries missing, running build.ps1..." -ForegroundColor Yellow
    & "$projectDir\build.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed. Aborting group tests."
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

function Extract-Match($text, $pattern) {
    $str = if ($text -is [array]) { $text -join "`n" } else { [string]$text }
    if ($str -match $pattern) {
        return $matches[1].Trim()
    }
    return ""
}

function Match-Text($text, $pattern) {
    $str = if ($text -is [array]) { $text -join "`n" } else { [string]$text }
    return ($str -match $pattern)
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Starting StreamForge Milestone 5 Consumer Groups Test Suite" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 0. Run C++ Unit Tests First
Write-Host "`n--- Running C++ Group Coordinator & Protocol Unit Tests ---" -ForegroundColor Cyan
& $groupTestsExe
Report-Check "C++ Group Coordinator Unit Tests (streamforge_group_tests.exe)" ($LASTEXITCODE -eq 0)

& $protoTestsExe
Report-Check "C++ Protocol Unit Tests (streamforge_protocol_tests.exe)" ($LASTEXITCODE -eq 0)

$port = 9098
$tempDataDir = Join-Path $projectDir "temp_group_test_data"
if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

$serverProc = $null

function Start-Broker($p = $port, $dir = $tempDataDir, $sync = "false", $minSess = 1000, $maxSess = 60000, $reaper = 200) {
    $proc = Start-Process -FilePath $serverExe -ArgumentList `
        "--port", $p, `
        "--data-dir", "`"$dir`"", `
        "--sync-on-append", $sync, `
        "--group-min-session-ms", $minSess, `
        "--group-max-session-ms", $maxSess, `
        "--reaper-interval-ms", $reaper, `
        "--workers", 4, `
        "--log-level", "WARN" `
        -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 600
    return $proc
}

try {
    Write-Host "`nStarting StreamForge Broker on port $port..." -ForegroundColor Cyan
    $serverProc = Start-Broker

    # -------------------------------------------------------------------------
    # TEST 1: JOIN / DESCRIBE / LEAVE BASICS
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 1: Join / Describe / Leave Basics ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic t_basic 4 | Out-Null

    $joinOut = & $cliExe --port $port join-group grp_basic t_basic --session-timeout-ms 5000
    $memberId = Extract-Match $joinOut "Member ID:\s*([^\r\n]+)"
    Report-Check "CLI join-group creates member and assigns partitions" ((Match-Text $joinOut "Joined group 'grp_basic' successfully") -and ($memberId -ne ""))

    $descOut = & $cliExe --port $port describe-group grp_basic
    Report-Check "CLI describe-group reflects joined member and Stable state" ((Match-Text $descOut "State:\s*Stable") -and (Match-Text $descOut "Generation:\s*1") -and (Match-Text $descOut $memberId))

    $leaveOut = & $cliExe --port $port leave-group grp_basic $memberId
    Report-Check "CLI leave-group succeeds" (Match-Text $leaveOut "Leave group OK")

    $descEmpty = & $cliExe --port $port describe-group grp_basic
    Report-Check "CLI describe-group reflects Empty state after leave" ((Match-Text $descEmpty "State:\s*Empty") -and (Match-Text $descEmpty "Members \(0\)"))

    # -------------------------------------------------------------------------
    # TEST 2: SINGLE CONSUMER READS ALL 3,000 RECORDS ACROSS 6 PARTITIONS (0 LAG)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 2: Single Consumer Reads 3,000 Records Across 6 Partitions ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic sc_topic 6 | Out-Null
    & $cliExe --port $port produce-many sc_topic 3000 --partition -1 --key-prefix user_ --batch 100 --size 64 | Out-Null

    $c1Log = Join-Path $projectDir "temp_c1_output.txt"
    if (Test-Path $c1Log) { Remove-Item $c1Log -Force }

    $c1Proc = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "consume-group", "sc_grp", "sc_topic", "--commit-every", "100", "--idle-exit", "3" -RedirectStandardOutput $c1Log -PassThru -NoNewWindow
    $c1Proc.WaitForExit(15000)

    $c1Lines = Get-Content $c1Log -ErrorAction SilentlyContinue
    $c1Count = ($c1Lines | Where-Object { $_ -match "^RECORD" }).Count
    Write-Host "Single consumer processed $c1Count records" -ForegroundColor Gray
    Report-Check "Single consumer consumed all 3,000 records" ($c1Count -eq 3000)

    $lagOut = & $cliExe --port $port group-lag sc_grp sc_topic
    $lagTotal = 0
    foreach ($line in ($lagOut -split "`r?`n")) {
        if ($line -match "lag=(\d+)") {
            $lagTotal += [int]$matches[1]
        }
    }
    Write-Host "Group sc_grp total lag: $lagTotal" -ForegroundColor Gray
    Report-Check "Group lag is 0 across all 6 partitions" ($lagTotal -eq 0)

    if (Test-Path $c1Log) { Remove-Item $c1Log -Force -ErrorAction SilentlyContinue }

    # -------------------------------------------------------------------------
    # TEST 3: TWO CONSUMERS SPLIT 3+3 PARTITIONS, 3,000 RECORDS NO DUPS / GAPS
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 3: Two Consumers Split 3+3 Partitions (3,000 Records) ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic tc_topic 6 | Out-Null
    & $cliExe --port $port produce-many tc_topic 3000 --partition -1 --key-prefix key_ --batch 100 --size 64 | Out-Null

    $tc1Log = Join-Path $projectDir "temp_tc1.txt"
    $tc2Log = Join-Path $projectDir "temp_tc2.txt"
    if (Test-Path $tc1Log) { Remove-Item $tc1Log -Force }
    if (Test-Path $tc2Log) { Remove-Item $tc2Log -Force }

    $j1 = & $cliExe --port $port join-group tc_grp tc_topic --session-timeout-ms 10000
    $m1 = Extract-Match $j1 "Member ID:\s*([^\r\n]+)"
    $j2 = & $cliExe --port $port join-group tc_grp tc_topic --session-timeout-ms 10000
    $m2 = Extract-Match $j2 "Member ID:\s*([^\r\n]+)"

    $p1 = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "consume-group", "tc_grp", "tc_topic", "--member", $m1, "--commit-every", "50", "--idle-exit", "3" -RedirectStandardOutput $tc1Log -PassThru -NoNewWindow
    $p2 = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "consume-group", "tc_grp", "tc_topic", "--member", $m2, "--commit-every", "50", "--idle-exit", "3" -RedirectStandardOutput $tc2Log -PassThru -NoNewWindow

    $p1.WaitForExit(15000)
    $p2.WaitForExit(15000)

    $lines1 = Get-Content $tc1Log -ErrorAction SilentlyContinue | Where-Object { $_ -match "^RECORD" }
    $lines2 = Get-Content $tc2Log -ErrorAction SilentlyContinue | Where-Object { $_ -match "^RECORD" }

    $seen = New-Object 'System.Collections.Generic.HashSet[string]'
    $duplicates = 0
    foreach ($l in ($lines1 + $lines2)) {
        if ($l -match "partition=(\d+)\s+offset=(\d+)") {
            $key = "$($matches[1]):$($matches[2])"
            if (-not $seen.Add($key)) {
                $duplicates++
            }
        }
    }

    Write-Host "Consumer 1 read $($lines1.Count) records, Consumer 2 read $($lines2.Count) records. Total unique: $($seen.Count), Duplicates: $duplicates" -ForegroundColor Gray
    Report-Check "Two consumers split partitions and processed 3,000 records without duplicates or gaps" ($seen.Count -eq 3000 -and $duplicates -eq 0)

    if (Test-Path $tc1Log) { Remove-Item $tc1Log -Force -ErrorAction SilentlyContinue }
    if (Test-Path $tc2Log) { Remove-Item $tc2Log -Force -ErrorAction SilentlyContinue }

    # -------------------------------------------------------------------------
    # TEST 4: THIRD CONSUMER JOINS (2/2/2 REBALANCE) THEN ONE LEAVES GRACEFULLY
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 4: Dynamic Rebalance (3+3 -> 2/2/2 -> 3+3) ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic reb_topic 6 | Out-Null

    $j1 = & $cliExe --port $port join-group reb_grp reb_topic --session-timeout-ms 10000
    $m1 = Extract-Match $j1 "Member ID:\s*([^\r\n]+)"
    $j2 = & $cliExe --port $port join-group reb_grp reb_topic --session-timeout-ms 10000
    $m2 = Extract-Match $j2 "Member ID:\s*([^\r\n]+)"
    $j3 = & $cliExe --port $port join-group reb_grp reb_topic --session-timeout-ms 10000
    $m3 = Extract-Match $j3 "Member ID:\s*([^\r\n]+)"

    $d3 = & $cliExe --port $port describe-group reb_grp
    $match222 = (Match-Text $d3 "Assigned partitions \(2\)")
    Report-Check "Third consumer causes 2/2/2 partition split across 3 members" ($match222 -and (Match-Text $d3 "Members \(3\)"))

    # Member 3 leaves gracefully
    & $cliExe --port $port leave-group reb_grp $m3 | Out-Null

    # Rebalance triggered on next describe/join
    $d2 = & $cliExe --port $port describe-group reb_grp
    $match33 = (Match-Text $d2 "Assigned partitions \(3\)")
    Report-Check "Graceful leave causes rebalance back to 3+3 partitions across 2 members" ($match33 -and (Match-Text $d2 "Members \(2\)"))

    & $cliExe --port $port leave-group reb_grp $m1 | Out-Null
    & $cliExe --port $port leave-group reb_grp $m2 | Out-Null

    # -------------------------------------------------------------------------
    # TEST 5: FORCE KILL CONSUMER -> SESSION TIMEOUT + REAPER -> SURVIVOR REBALANCES
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 5: Force Kill Consumer & Reaper Expiration ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic kill_topic 4 | Out-Null
    $k1 = & $cliExe --port $port join-group kill_grp kill_topic --session-timeout-ms 10000
    $km1 = Extract-Match $k1 "Member ID:\s*([^\r\n]+)"
    $k2 = & $cliExe --port $port join-group kill_grp kill_topic --session-timeout-ms 1500
    $km2 = Extract-Match $k2 "Member ID:\s*([^\r\n]+)"

    # km2 silently disappears (no heartbeat, no leave)
    Write-Host "Simulating dead consumer km2 (waiting for session timeout > 1.5s)..." -ForegroundColor Gray
    Start-Sleep -Milliseconds 2200

    # km1 rejoins and gets all 4 partitions
    $rejoin = & $cliExe --port $port join-group kill_grp kill_topic --member $km1 --session-timeout-ms 5000
    $rejoinHas4 = (Match-Text $rejoin "Assigned partitions \(4\)")
    Report-Check "Session timeout expires dead member, surviving member receives all partitions" ($rejoinHas4)

    & $cliExe --port $port leave-group kill_grp $km1 | Out-Null

    # -------------------------------------------------------------------------
    # TEST 6: AT-LEAST-ONCE CRASH RECOVERY (--crash-after 250)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 6: Crash Recovery (--crash-after 250, Resume From 200) ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic crash_topic 1 | Out-Null
    & $cliExe --port $port produce-many crash_topic 500 --partition 0 --batch 50 --size 32 | Out-Null

    $crashLog = Join-Path $projectDir "temp_crash1.txt"
    if (Test-Path $crashLog) { Remove-Item $crashLog -Force }

    # First consumer crashes after exactly 250 records with commit-every 100 -> commits 0..99 (offset 100) and 100..199 (offset 200)
    $procCrash = Start-Process -FilePath $cliExe -ArgumentList `
        "--port", $port, "consume-group", "crash_grp", "crash_topic", `
        "--commit-every", "100", "--crash-after", "250", "--session-timeout-ms", "2000" `
        -RedirectStandardOutput $crashLog -PassThru -NoNewWindow
    $procCrash.WaitForExit(10000)

    $firstCount = (Get-Content $crashLog -ErrorAction SilentlyContinue | Where-Object { $_ -match "^RECORD" }).Count
    Write-Host "Crashing consumer produced $firstCount records before immediate exit" -ForegroundColor Gray

    # Check committed offset: must be exactly 200
    $foOut = & $cliExe --port $port fetch-offsets crash_grp crash_topic
    $committedOffset = Extract-Match $foOut "Partition 0:\s*(\d+)"
    Write-Host "Committed offset after crash: $committedOffset" -ForegroundColor Gray
    Report-Check "Committed offset after crash-after 250 is exactly 200" ($committedOffset -eq "200")

    # Wait for session timeout of the crashed consumer
    Start-Sleep -Milliseconds 2200

    # Second consumer starts up and reads remaining records
    $recovLog = Join-Path $projectDir "temp_crash2.txt"
    if (Test-Path $recovLog) { Remove-Item $recovLog -Force }

    $procRecov = Start-Process -FilePath $cliExe -ArgumentList `
        "--port", $port, "consume-group", "crash_grp", "crash_topic", `
        "--commit-every", "50", "--idle-exit", "3" `
        -RedirectStandardOutput $recovLog -PassThru -NoNewWindow
    $procRecov.WaitForExit(15000)

    $recovLines = Get-Content $recovLog -ErrorAction SilentlyContinue | Where-Object { $_ -match "^RECORD" }
    $firstRecovOffset = Extract-Match $recovLines[0] "offset=(\d+)"

    Write-Host "Recovering consumer read $($recovLines.Count) records starting at offset $firstRecovOffset" -ForegroundColor Gray
    Report-Check "Second consumer resumes from committed offset 200 (at-least-once recovery, 300 records)" `
        ($firstRecovOffset -eq "200" -and $recovLines.Count -eq 300)

    if (Test-Path $crashLog) { Remove-Item $crashLog -Force -ErrorAction SilentlyContinue }
    if (Test-Path $recovLog) { Remove-Item $recovLog -Force -ErrorAction SilentlyContinue }

    # -------------------------------------------------------------------------
    # TEST 7: FENCING VALIDATION (ERRORS 15, 16, 8, AND STANDALONE COMMIT)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 7: Coordinator Fencing Validation ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic fence_topic 2 | Out-Null
    & $cliExe --port $port produce-many fence_topic 10 --partition 0 | Out-Null
    & $cliExe --port $port produce-many fence_topic 10 --partition 1 | Out-Null

    $fj = & $cliExe --port $port join-group fence_grp fence_topic --session-timeout-ms 10000
    $fmId = Extract-Match $fj "Member ID:\s*([^\r\n]+)"

    # Stale generation (e.g. gen 99 vs gen 1) -> Error 15
    $outStale = & $cliExe --port $port commit-offset fence_grp fence_topic 0 5 --member $fmId --generation 99 2>&1
    Report-Check "Stale generation commit rejected with Error 15" (Match-Text $outStale "Error \(code 15\)")

    # Partition not assigned to member -> Error 16
    & $cliExe --port $port create-topic unassigned_topic 1 | Out-Null
    $outUnassigned = & $cliExe --port $port commit-offset fence_grp unassigned_topic 0 1 --member $fmId --generation 1 2>&1
    Report-Check "Unassigned partition commit rejected with Error 16" (Match-Text $outUnassigned "Error \(code 16\)")

    # Offset out of range (> high watermark 10) -> Error 8
    $outOorCommit = & $cliExe --port $port commit-offset fence_grp fence_topic 0 9999 --member $fmId --generation 1 2>&1
    Report-Check "Out-of-range offset commit rejected with Error 8" (Match-Text $outOorCommit "Error \(code 8\)")

    # Standalone commit without member/generation -> OK
    $outStandalone = & $cliExe --port $port commit-offset standalone_grp fence_topic 0 5
    Report-Check "Standalone commit without group membership succeeds" (Match-Text $outStandalone "Commit offset OK")

    $foStand = & $cliExe --port $port fetch-offsets standalone_grp fence_topic --partitions 0
    Report-Check "Standalone committed offset retrievable" (Match-Text $foStand "Partition 0:\s*5")

    # -------------------------------------------------------------------------
    # TEST 8: OFFSETS SURVIVE GRACEFUL BROKER RESTART & HARD KILL
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 8: Committed Offsets Durability Across Restarts ---" -ForegroundColor Cyan
    # Graceful stop via protocol shutdown
    & $cliExe --port $port shutdown | Out-Null
    $serverProc.WaitForExit(5000)

    # Restart broker
    $serverProc = Start-Broker
    $foRestart = & $cliExe --port $port fetch-offsets standalone_grp fence_topic --partitions 0
    Report-Check "Committed offset survives graceful broker restart" (Match-Text $foRestart "Partition 0:\s*5")

    # Hard kill broker
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit(3000)

    # Restart broker with sync-on-append
    $serverProc = Start-Broker -sync "true"
    $foHardKill = & $cliExe --port $port fetch-offsets standalone_grp fence_topic --partitions 0
    Report-Check "Committed offset survives hard kill (Stop-Process -Force)" (Match-Text $foHardKill "Partition 0:\s*5")

    # -------------------------------------------------------------------------
    # TEST 9: RESERVED TOPIC RESTRICTION (__consumer_offsets)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 9: Reserved Topic Restrictions (__consumer_offsets) ---" -ForegroundColor Cyan
    $resCreate = & $cliExe --port $port create-topic __consumer_offsets 1 2>&1
    Report-Check "create-topic __consumer_offsets returns Error Code 6" (Match-Text $resCreate "Error \(code 6\)")

    $resProduce = & $cliExe --port $port produce __consumer_offsets "test" 2>&1
    Report-Check "produce to __consumer_offsets returns Error Code 6" (Match-Text $resProduce "Error \(code 6\)")

    $resFetch = & $cliExe --port $port fetch __consumer_offsets 0 0 2>&1
    Report-Check "fetch from __consumer_offsets returns Error Code 6" (Match-Text $resFetch "Error \(code 6\)")

    $resDesc = & $cliExe --port $port describe __consumer_offsets 2>&1
    Report-Check "describe __consumer_offsets returns Error Code 6" (Match-Text $resDesc "Error \(code 6\)")

    $topicsList = & $cliExe --port $port topics
    Report-Check "Reserved internal topic '__consumer_offsets' hidden from list topics" (-not (Match-Text $topicsList "__consumer_offsets"))

    # -------------------------------------------------------------------------
    # TEST 10: MALFORMED PROTOCOL BODY DOES NOT CLOSE CONNECTION
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 10: Malformed Body Error 10 Keeps Socket Open ---" -ForegroundColor Cyan
    $malOut = & $cliExe --port $port malformed-produce 2>&1
    Report-Check "Malformed request returns Error Code 10 and follow-up PING succeeds on open connection" `
        ($LASTEXITCODE -eq 0 -and (Match-Text $malOut "subsequent PING succeeded"))

    # -------------------------------------------------------------------------
    # TEST 11: BROKER RESTART WITH ACTIVE MEMBERS -> ERROR 14 -> AUTO RE-JOIN
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 11: Broker Restart With Active Consumer -> Auto Re-Join ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic restart_topic 2 | Out-Null
    & $cliExe --port $port produce-many restart_topic 200 --partition 0 --batch 50 | Out-Null

    $restLog = Join-Path $projectDir "temp_restart_client.txt"
    if (Test-Path $restLog) { Remove-Item $restLog -Force }

    $clientProc = Start-Process -FilePath $cliExe -ArgumentList `
        "--port", $port, "consume-group", "rest_grp", "restart_topic", `
        "--commit-every", "50", "--idle-exit", "8" `
        -RedirectStandardOutput $restLog -PassThru -NoNewWindow
    Start-Sleep -Milliseconds 600

    # Restart broker while client is running
    Write-Host "Restarting broker while client is actively consuming..." -ForegroundColor Gray
    Stop-Process -Id $serverProc.Id -Force
    $serverProc.WaitForExit(3000)
    $serverProc = Start-Broker

    # Produce 100 more records
    Start-Sleep -Milliseconds 1000
    & $cliExe --port $port produce-many restart_topic 100 --partition 0 --batch 50 | Out-Null

    $clientProc.WaitForExit(12000)
    $restLines = Get-Content $restLog -ErrorAction SilentlyContinue | Where-Object { $_ -match "^RECORD" }
    Write-Host "Client recovered and processed $($restLines.Count) records across broker restart" -ForegroundColor Gray
    Report-Check "Consumer auto-reconnects, handles Error 14, rejoins, and consumes remaining records" ($restLines.Count -eq 300)

    if (Test-Path $restLog) { Remove-Item $restLog -Force -ErrorAction SilentlyContinue }

    # -------------------------------------------------------------------------
    # TEST 12: HANDLE LEAKS ACROSS 200 JOIN/LEAVE CYCLES
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 12: Handle Leak Test Across 200 Join/Leave Cycles ---" -ForegroundColor Cyan
    & $cliExe --port $port create-topic leak_topic 2 | Out-Null
    $procBefore = Get-Process -Id $serverProc.Id
    $handlesBefore = $procBefore.HandleCount

    for ($c = 0; $c -lt 200; $c++) {
        $jOut = & $cliExe --port $port join-group leak_grp leak_topic --session-timeout-ms 10000
        $m = Extract-Match $jOut "Member ID:\s*([^\r\n]+)"
        if ($m -ne "") {
            & $cliExe --port $port leave-group leak_grp $m | Out-Null
        }
    }

    $procAfter = Get-Process -Id $serverProc.Id
    $handlesAfter = $procAfter.HandleCount
    $diff = [math]::Abs($handlesAfter - $handlesBefore)
    Write-Host "Server handle count before: $handlesBefore, after: $handlesAfter (diff: $diff)" -ForegroundColor Gray
    Report-Check "Handle leak test (200 join/leave cycles, diff <= 15)" ($diff -le 15)

    # -------------------------------------------------------------------------
    # TEST 13: M4 THREAD-COUNT STABILITY (200 IDLE CONNECTIONS HELD)
    # -------------------------------------------------------------------------
    Write-Host "`n--- TEST 13: Thread Count Stability Under 200 Idle Connections ---" -ForegroundColor Cyan
    $pObj = Get-Process -Id $serverProc.Id
    $threadsBefore = $pObj.Threads.Count

    $holder = Start-Process -FilePath $cliExe -ArgumentList "--port", $port, "hold-connections", "200", "5" -PassThru -NoNewWindow
    Start-Sleep -Seconds 2

    $pObj.Refresh()
    $threadsDuring = $pObj.Threads.Count
    $threadDiff = [math]::Abs($threadsDuring - $threadsBefore)
    Write-Host "Server threads before: $threadsBefore, during 200 idle conns: $threadsDuring (diff: $threadDiff)" -ForegroundColor Gray
    Report-Check "Thread count remains constant with 200 connections held (diff <= 2)" ($threadDiff -le 2)

    $holder.WaitForExit(6000)

} finally {
    Write-Host "`nStopping server process..." -ForegroundColor Cyan
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $tempDataDir) {
        Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "--- MILESTONE 5 TEST SUMMARY ---" -ForegroundColor Cyan
Write-Host "Passed: $script:passed" -ForegroundColor Green
if ($script:failed -gt 0) {
    Write-Host "Failed: $script:failed" -ForegroundColor Red
    exit 1
} else {
    Write-Host "Failed: $script:failed" -ForegroundColor Green
    Write-Host "All Milestone 5 tests passed successfully!" -ForegroundColor Green
    exit 0
}
