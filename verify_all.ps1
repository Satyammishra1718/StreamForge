$ErrorActionPreference = "Stop"

$projectDir = Get-Location

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Running StreamForge Master Verification Suite" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$suites = @(
    @{ Name = "Milestone 1 Core Network & Protocol"; Script = ".\test.ps1" },
    @{ Name = "Milestone 2 Storage Engine"; Script = ".\test_storage.ps1" },
    @{ Name = "Milestone 3 Broker Network Integration"; Script = ".\test_broker.ps1" },
    @{ Name = "Milestone 4 Concurrency, I/O Loop & Thread Pool"; Script = ".\test_concurrency.ps1" },
    @{ Name = "Milestone 5 Consumer Groups & Offset Commits"; Script = ".\test_groups.ps1" },
    @{ Name = "Milestone 6 Crash Recovery"; Script = ".\test_crash_recovery.ps1" },
    @{ Name = "Milestone 6 Retention & GC"; Script = ".\test_retention.ps1" }
)

$passedSuites = 0
$failedSuites = 0

foreach ($s in $suites) {
    Write-Host "`n>>> Running $($s.Name) ($($s.Script))..." -ForegroundColor Yellow
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & powershell -ExecutionPolicy Bypass -File $s.Script
    $sw.Stop()

    if ($LASTEXITCODE -eq 0) {
        Write-Host ">>> [PASS] $($s.Name) (elapsed: $($sw.ElapsedMilliseconds) ms)" -ForegroundColor Green
        $passedSuites++
    } else {
        Write-Host ">>> [FAIL] $($s.Name) failed with exit code $LASTEXITCODE" -ForegroundColor Red
        $failedSuites++
    }
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "STREAMFORGE MASTER VERIFICATION SUMMARY" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Suites Passed: $passedSuites / $($suites.Count)" -ForegroundColor Green

if ($failedSuites -gt 0) {
    Write-Host "Suites Failed: $failedSuites" -ForegroundColor Red
    exit 1
} else {
    Write-Host "All suites passed cleanly with exit code 0!" -ForegroundColor Green
    exit 0
}
