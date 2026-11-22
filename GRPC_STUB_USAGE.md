# gRPC Stub Usage

## 目的

这份文档说明如何把当前项目的 `order.proto` 生成给客户端使用的 Java stub。

当前 proto 文件位置：

- `src/main/proto/order.proto`

生成后的 Java 包名：

- `com.example.order.grpc`

客户端通常会拿到这几类生成物：

- protobuf message
- gRPC service stub

## 当前项目状态

项目的 `pom.xml` 已经接入：

- `protobuf-maven-plugin`
- `grpc-java` 代码生成

所以服务端自己编译时，Maven 可以生成：

- `target/generated-sources/protobuf/java`
- `target/generated-sources/protobuf/grpc-java`

但如果客户端想单独拿 stub，更直接的做法是用本机 `protoc` 单独生成。

## 本机 protoc 生成

项目已提供脚本：

- `scripts/generate-grpc-stubs.ps1`

默认输出目录：

- `generated/client-stubs/java`

### 前提

本机需要：

- `protoc`
- `protoc-gen-grpc-java`

说明：

- 运行脚本前需在本机安装 `protoc`，或者通过 `-ProtocPath` 传入可执行文件路径
- `grpc-java` 插件需要额外提供路径
- 如果当前工程已经执行过 Maven proto 生成，脚本会优先尝试使用：
  - `target/protoc-plugins/protoc-gen-grpc-java-1.71.0-windows-x86_64.exe`

### 用法

如果 `grpc-java` 插件已经在 `target/protoc-plugins` 下：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\generate-grpc-stubs.ps1
```

如果你本机有单独的 `protoc-gen-grpc-java`：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\generate-grpc-stubs.ps1 -GrpcJavaPluginPath "C:\tools\protoc-gen-grpc-java.exe"
```

如果 `protoc` 不在 `PATH` 里：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\generate-grpc-stubs.ps1 -ProtocPath "C:\tools\protoc.exe" -GrpcJavaPluginPath "C:\tools\protoc-gen-grpc-java.exe"
```

## 生成结果

生成后你会在下面目录看到客户端可用源码：

- `generated/client-stubs/java/com/example/order/grpc`

通常会包含：

- `OrderServiceGrpc.java`
- `PlaceOrderRequest.java`
- `PlaceOrderResponse.java`
- `CancelOrderRequest.java`
- `CancelOrderResponse.java`
- `GetBookRequest.java`
- `GetBookResponse.java`
- `Side.java`

## 客户端接入方式

Java 客户端有两种常见接法：

### 方式 1：直接拷贝生成源码

适合：

- 内部 demo
- 快速联调

做法：

- 把 `generated/client-stubs/java/com/example/order/grpc` 复制到客户端工程
- 客户端再补 gRPC 依赖

### 方式 2：单独做一个 proto/stub 模块

适合：

- 多个客户端共用
- 版本管理更规范

做法：

1. 新建独立模块，例如 `order-grpc-api`
2. 只保留 `proto` 和生成配置
3. 打包成 jar 给客户端引用

这比直接依赖服务端主工程更合理。

## 客户端最少依赖

如果客户端用 Java blocking stub，通常需要：

- `grpc-netty-shaded`
- `grpc-protobuf`
- `grpc-stub`
- `protobuf-java`

## 推荐做法

如果只是当前这个项目内联调：

- 直接运行 `scripts/generate-grpc-stubs.ps1`

如果后续会有多个客户端：

- 把 proto 和生成逻辑拆成独立 `api` 模块

这样版本边界会更清楚，也不会让客户端依赖服务端全部业务代码。
