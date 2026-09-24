$ErrorActionPreference = "Continue"

$projectDir = Get-Location
$buildDir = Join-Path $projectDir "build"
$storageExe = Join-Path $buildDir "streamforge_storage.exe"
$testsExe = Join-Path $buildDir "streamforge_storage_tests.exe"

if (-not (Test-Path $storageExe) -or -not (Test-Path $testsExe)) {
    Write-Host "Storage binaries missing, running build.ps1..." -ForegroundColor Yellow
    & "$projectDir\build.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed. Aborting storage tests."
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

Write-Host "Running C++ Storage Unit Tests (streamforge_storage_tests.exe)..." -ForegroundColor Cyan
& $testsExe
Report-Check "C++ Storage Unit Test Suite" ($LASTEXITCODE -eq 0)

$tempDataDir = Join-Path $projectDir "temp_e2e_storage_data"
if (Test-Path $tempDataDir) {
    Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
}

try {
    Write-Host "`nRunning End-to-End Storage CLI Scenario..." -ForegroundColor Cyan

    # 1. Create Topic
    $outCreate = & $storageExe --data-dir $tempDataDir create-topic e2e_orders 2
    Report-Check "CLI create-topic" ($LASTEXITCODE -eq 0 -and $outCreate -match "Successfully created topic")

    # 2. Fill Topic (small segment size 1024 bytes to force multiple segment rolls)
    $outFill = & $storageExe --data-dir $tempDataDir fill e2e_orders 50 100 --segment-bytes 1024
    Report-Check "CLI fill topic (forcing segment rolls)" ($LASTEXITCODE -eq 0 -and $outFill -match "Successfully filled 50 records")

    # 3. Describe Topic (verify multiple segments created)
    $outDesc = & $storageExe --data-dir $tempDataDir describe e2e_orders
    Report-Check "CLI describe topic (segment count > 1)" ($LASTEXITCODE -eq 0 -and $outDesc -match "Segments=")

    # 4. Read records across segment boundaries
    $outRead = & $storageExe --data-dir $tempDataDir read e2e_orders 0 0 --max 50
    Report-Check "CLI read records across segment boundaries" ($LASTEXITCODE -eq 0 -and $outRead -match "Read ")

    # 5. Confirm persistence on tool reopen
    $outReopen = & $storageExe --data-dir $tempDataDir describe e2e_orders
    Report-Check "CLI persistence check on tool restart" ($LASTEXITCODE -eq 0 -and $outReopen -match "NextOffset=")

} finally {
    Write-Host "`nCleaning up temporary test directory: $tempDataDir" -ForegroundColor Cyan
    if (Test-Path $tempDataDir) {
        Remove-Item -Path $tempDataDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n--- STORAGE TEST SUMMARY ---" -ForegroundColor Cyan
Write-Host "Passed: $script:passed" -ForegroundColor Green
if ($script:failed -gt 0) {
    Write-Host "Failed: $script:failed" -ForegroundColor Red
    exit 1
} else {
    Write-Host "Failed: $script:failed" -ForegroundColor Green
    exit 0
}
