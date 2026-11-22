param(
    [string]$ProtocPath = "protoc",
    [string]$GrpcJavaPluginPath = "",
    [string]$ProtoFile = "src/main/proto/order.proto",
    [string]$OutputDir = "generated/client-stubs/java"
)

$ErrorActionPreference = "Stop"

function Resolve-Executable([string]$CommandName) {
    $command = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        throw "未找到可执行文件: $CommandName"
    }
    return $command.Source
}

if ([string]::IsNullOrWhiteSpace($GrpcJavaPluginPath)) {
    $defaultPlugin = Join-Path $PSScriptRoot "..\\target\\protoc-plugins\\protoc-gen-grpc-java-1.71.0-windows-x86_64.exe"
    if (Test-Path $defaultPlugin) {
        $GrpcJavaPluginPath = (Resolve-Path $defaultPlugin).Path
    } else {
        throw "未提供 grpc-java 插件路径。请通过 -GrpcJavaPluginPath 指定 protoc-gen-grpc-java。"
    }
}

$resolvedProtoc = Resolve-Executable $ProtocPath
$resolvedProto = (Resolve-Path $ProtoFile).Path
$resolvedPlugin = (Resolve-Path $GrpcJavaPluginPath).Path
$resolvedOutDir = Join-Path (Get-Location) $OutputDir

New-Item -ItemType Directory -Force -Path $resolvedOutDir | Out-Null

& $resolvedProtoc `
    "--proto_path=$(Split-Path $resolvedProto -Parent)" `
    "--java_out=$resolvedOutDir" `
    "--grpc-java_out=$resolvedOutDir" `
    "--plugin=protoc-gen-grpc-java=$resolvedPlugin" `
    $resolvedProto

Write-Host "gRPC Java stub 已生成到: $resolvedOutDir"
