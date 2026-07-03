#!/usr/bin/env pwsh
# Prepare a local NuGet package for Timslite.
# Usage: pwsh wrapper/dotnet/scripts/prepare-publish.ps1 [-Release]
# Prerequisites: Rust toolchain with cross-compilation targets, .NET 8 SDK.

param(
    [switch]$Release
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$DotnetRoot = Split-Path -Parent $ScriptDir
$RepoRoot = Split-Path -Parent (Split-Path -Parent $DotnetRoot)

$Config = if ($Release) { "Release" } else { "Debug" }
$CargoFlag = if ($Release) { "--release" } else { "" }

function Get-CrateVersion {
    param([string]$ManifestPath)
    $content = Get-Content $ManifestPath -Raw
    if ($content -match 'version\s*=\s*"([^"]+)"') {
        return $Matches[1]
    }
    throw "Cannot read version from $ManifestPath"
}

function Build-NativeLib {
    param(
        [string]$Target,
        [string]$Rid
    )

    $nativeDir = Join-Path $DotnetRoot "native"
    Write-Host "Building native library for $Rid (target: $Target)..." -ForegroundColor Cyan

    $cargoArgs = @("build", "--target", $Target)
    if ($Release) {
        $cargoArgs += "--release"
    }

    & cargo @cargoArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed for $Target"
    }

    $targetDir = Join-Path $nativeDir "target" $Target
    $buildSubdir = if ($Release) { "release" } else { "debug" }
    $buildDir = Join-Path $targetDir $buildSubdir

    $runtimesDir = Join-Path $DotnetRoot "src" "Timslite" "runtimes" $Rid "native"
    New-Item -ItemType Directory -Path $runtimesDir -Force | Out-Null

    if ($Rid.StartsWith("win")) {
        $libName = "timslite_dotnet.dll"
    } elseif ($Rid.StartsWith("osx")) {
        $libName = "libtimslite_dotnet.dylib"
    } else {
        $libName = "libtimslite_dotnet.so"
    }

    $src = Join-Path $buildDir $libName
    if (-not (Test-Path $src)) {
        throw "Expected native library not found: $src"
    }

    Copy-Item $src (Join-Path $runtimesDir $libName) -Force
    Write-Host "  Copied to $runtimesDir/$libName" -ForegroundColor Green
}

Write-Host "Checking version alignment..." -ForegroundColor Yellow
$rootVersion = Get-CrateVersion (Join-Path $RepoRoot "Cargo.toml")
$nativeVersion = Get-CrateVersion (Join-Path $DotnetRoot "native" "Cargo.toml")
$csprojPath = Join-Path $DotnetRoot "src" "Timslite" "Timslite.csproj"
$csprojContent = Get-Content $csprojPath -Raw

if ($rootVersion -ne $nativeVersion) {
    Write-Host "ERROR: Version mismatch!" -ForegroundColor Red
    Write-Host "  Root crate:    $rootVersion"
    Write-Host "  Native crate:  $nativeVersion"
    exit 1
}

if (-not ($csprojContent -match "<Version>([^<]+)</Version>")) {
    Write-Host "ERROR: Cannot read Version from csproj" -ForegroundColor Red
    exit 1
}
$csprojVersion = $Matches[1]
if ($csprojVersion -ne $rootVersion) {
    Write-Host "ERROR: Version mismatch!" -ForegroundColor Red
    Write-Host "  Root crate: $rootVersion"
    Write-Host "  Csproj:     $csprojVersion"
    exit 1
}

Write-Host "  All versions aligned: $rootVersion" -ForegroundColor Green

$targets = @(
    @{ Target = "x86_64-pc-windows-msvc";  Rid = "win-x64" },
    @{ Target = "aarch64-pc-windows-msvc";  Rid = "win-arm64" },
    @{ Target = "x86_64-unknown-linux-gnu"; Rid = "linux-x64" },
    @{ Target = "aarch64-unknown-linux-gnu"; Rid = "linux-arm64" },
    @{ Target = "x86_64-apple-darwin";      Rid = "osx-x64" },
    @{ Target = "aarch64-apple-darwin";     Rid = "osx-arm64" }
)

foreach ($t in $targets) {
    Build-NativeLib -Target $t.Target -Rid $t.Rid
}

Write-Host "`nPacking NuGet package..." -ForegroundColor Yellow
$packArgs = @("pack", $csprojPath, "-c", $Config, "--no-build")
& dotnet @packArgs
if ($LASTEXITCODE -ne 0) {
    throw "dotnet pack failed"
}

Write-Host "`nDone! Package is in src/Timslite/bin/$Config/" -ForegroundColor Green
