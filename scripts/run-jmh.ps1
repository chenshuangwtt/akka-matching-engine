param(
    [string]$SettingsPath = (Join-Path $env:MAVEN_HOME 'conf/settings.xml'),
    [string[]]$JmhArgs = @('MatchingEngineJmhBenchmark.orderBook', '-wi', '3', '-i', '5', '-f', '1')
)

$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if (-not (Test-Path -LiteralPath $SettingsPath -PathType Leaf)) {
    throw "Maven settings.xml not found: $SettingsPath"
}

Push-Location $projectRoot
try {
    & mvn -o -s $SettingsPath compile org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath '-Dmdep.outputFile=target/jmh-classpath.txt'
    if ($LASTEXITCODE -ne 0) { throw "Offline Maven compile/classpath failed: $LASTEXITCODE" }
    $dependencies = (Get-Content -LiteralPath 'target/jmh-classpath.txt' -Raw).Trim()
    $classpath = (Join-Path $projectRoot 'target/classes') + [System.IO.Path]::PathSeparator + $dependencies
    & java -cp $classpath org.openjdk.jmh.Main @JmhArgs
    if ($LASTEXITCODE -ne 0) { throw "Forked JMH run failed: $LASTEXITCODE" }
} finally {
    Pop-Location
}
