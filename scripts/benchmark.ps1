param(
    [string]$OutputDirectory = "benchmark-results"
)

$ErrorActionPreference = "Stop"
$repository = Split-Path -Parent $PSScriptRoot
$output = Join-Path $repository $OutputDirectory
New-Item -ItemType Directory -Force -Path $output | Out-Null

$metadata = @(
    "timestamp_utc=$([DateTime]::UtcNow.ToString('o'))"
    "git_revision=$(git -C $repository rev-parse HEAD)"
    "rustc=$(rustc --version)"
    "cargo=$(cargo --version)"
    "cpu=$((Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name).Trim())"
    "logical_processors=$((Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors)"
    "memory_bytes=$((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory)"
)
Set-Content -LiteralPath (Join-Path $output "environment.txt") -Value $metadata

Push-Location $repository
try {
    cargo test --release --all-targets
    cargo bench --bench engine
}
finally {
    Pop-Location
}

Write-Host "Criterion report: $repository\target\criterion\report\index.html"
Write-Host "Environment:      $output\environment.txt"
