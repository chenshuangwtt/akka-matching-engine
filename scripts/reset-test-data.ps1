param(
    [Parameter(Mandatory = $true)]
    [string]$TestDirectory
)

$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$targetRoot = [System.IO.Path]::GetFullPath((Join-Path $projectRoot 'target'))
$candidate = [System.IO.Path]::GetFullPath($TestDirectory)
$prefix = $targetRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar

if (-not $candidate.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'Only an explicitly specified directory inside this project target/ may be reset.'
}
if (-not (Test-Path -LiteralPath $candidate -PathType Container)) {
    throw "Test directory does not exist: $candidate"
}

$current = $candidate
while ($current.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    $item = Get-Item -LiteralPath $current -Force
    if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Refusing to reset a reparse-point directory: $current"
    }
    $current = [System.IO.Path]::GetDirectoryName($current)
}

Remove-Item -LiteralPath $candidate -Recurse -Force
Write-Output "Reset test directory: $candidate"
