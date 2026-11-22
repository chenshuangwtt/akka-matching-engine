param(
    [string]$SettingsPath = (Join-Path $env:MAVEN_HOME 'conf/settings.xml'),
    [int]$Measured = 400,
    [int]$Warmup = 100,
    [int]$OrdersPerSymbol = 2500,
    [int]$Symbols = 4
)

$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if (-not (Test-Path -LiteralPath $SettingsPath -PathType Leaf)) {
    throw "Maven settings.xml not found: $SettingsPath"
}

Push-Location $projectRoot
try {
    & mvn -o -s $SettingsPath compile org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath '-Dmdep.outputFile=target/p3-classpath.txt'
    if ($LASTEXITCODE -ne 0) { throw "Offline Maven compile/classpath failed: $LASTEXITCODE" }
    $dependencies = (Get-Content -LiteralPath 'target/p3-classpath.txt' -Raw).Trim()
    $classpath = (Join-Path $projectRoot 'target/classes') + [System.IO.Path]::PathSeparator + $dependencies
    & java -cp $classpath com.example.matching.benchmark.P3LoadProbe $Measured $Warmup $OrdersPerSymbol $Symbols
    if ($LASTEXITCODE -ne 0) { throw "P3 load probe failed: $LASTEXITCODE" }
} finally {
    Pop-Location
}
