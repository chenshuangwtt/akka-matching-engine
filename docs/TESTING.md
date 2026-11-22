# 测试与基准运行

## 环境与离线回归

项目以 JDK 21、Maven 3.9.9 和已缓存依赖的 Windows 环境验证。先确认 `java -version`、`javac -version`、`mvn -version`。本机 `settings.xml` 指向 `D:\Maven`，统一运行：

```powershell
mvn -o -s "D:\java\apache-maven-3.9.9\conf\settings.xml" test
```

不运行 `mvnw`、`mvnw.cmd`、`clean`，也不在测试过程中更改 Maven 配置或联网补依赖。其他机器可用自己配置的系统 Maven；首次获取依赖不属于本仓库已验证的离线基线。

2026-11-22 的 P3 完整离线回归结果：**48 个测试，48 通过，0 失败，0 错误，0 跳过**。测试数量随代码变化时以 `target/surefire-reports/TEST-*.xml` 的实际结果为准。

| 测试类 | 数量 | 主要验证 |
| --- | ---: | --- |
| `AkkaMatchingEngineApplicationTests` | 1 | Spring Boot 上下文及 gRPC 服务启动 |
| `SymbolActorPersistenceTest` | 11 | 写入确认、写入失败、恢复故障、顺序、检查点阈值和状态 |
| `OrderEventStoreCheckpointTest` | 7 | 检查点、损坏和残缺 Journal、不兼容格式拒绝、Windows 替换、成交 ID 多次恢复 |
| `ProcessCrashAcceptanceTest` | 5 | 独立 JVM 强制退出及恢复 |
| `ActorFaultAcceptanceTest` | 4 | Actor 重启、根 Actor 引用、未完成请求与持久化 Actor 故障 |
| `SymbolActorIdempotencyTest` | 12 | 同键重试、冲突、并发、成交/撤单/重启、无效订单、成交 ID、容量边界 |
| `OrderBookConsistencyTest` | 5 | 价格时间优先、剩余量、撤单、快照完整性、跨交易对成交 ID |
| `OrderIngressIdempotencyTest` | 2 | HTTP/gRPC 键透传、冲突、参数错误映射和斜杠交易对查询 |
| `OrderOverloadAcceptanceTest` | 1 | 写盘屏障下的有界准入、立即拒绝、已接受订单与幂等重试 |

仅运行某类测试时，PowerShell 应将含逗号的 `-Dtest` 参数整体加引号，例如：

```powershell
mvn -o -s "D:\java\apache-maven-3.9.9\conf\settings.xml" "-Dtest=ProcessCrashAcceptanceTest,ActorFaultAcceptanceTest" test
```

## 故障注入位置

`ProcessCrashAcceptanceTest` 用 `ProcessBuilder` 启动独立 Java 子进程；子进程到达指定代码屏障时写入标记，父进程据此强制终止。每例使用独立 JUnit 临时目录保存 Journal、Snapshot、信号和日志，不使用仓库默认 `data/events`，也不以随机睡眠选择故障时刻。

| 屏障名 | 终止位置 | 重启后的关键检查 |
| --- | --- | --- |
| `journal-before-force` | Journal 写入后、`FileChannel.force` 前 | 未确认结果可能已落盘或未落盘；恢复后同键重试不得重复成交 |
| `before-business-reply` | 已持久化并应用、客户端回复前 | 原事件恢复，重试关联原订单 |
| `snapshot-temp-partial` | Snapshot 临时文件只写入一部分 | 正式快照未替换，按完整 Journal 恢复 |
| `snapshot-committed` | Snapshot 原子替换后、清理 Journal 前 | 跳过快照已含事件，不重复回放 |
| `journal-cleanup-temp` | Journal 临时清理文件写入阶段 | 仍能按检查点恢复 |

测试会记录故障前已确认序号、恢复序号、活动订单数、剩余数量、状态、回放成交量和重试结果。它验证的是**进程强制退出**；物理断电、磁盘控制器缓存及 Windows 目录元数据持久性没有被证明。若恢复数据残缺且无法判定安全状态，预期行为是拒绝撮合。

Actor 测试分别覆盖 `SymbolActor` 异常重启、永久停止后的根 Actor 引用清理、回复前退出的未知结果，以及 `PersistenceActor` 故障后的拒绝服务。恢复完成前的请求不会直接在空订单簿上执行。

## 手动接口检查

使用独立空目录启动服务，再执行 README 的 HTTP 下单、重复下单、部分成交、撤单和订单簿示例；用 `OrderGrpcClientExample` 验证 gRPC 三个基础方法。默认 HTTP `8080`、gRPC `50051`。接口例子是烟雾检查，不能代替故障测试。

最终发布验收在新的 `target/release-smoke-20261122-1741/events` 目录启动 HTTP `18080` / gRPC `15051`：HTTP 验证 `SMOKE/USDT` 下单、同键重试、409 冲突、400 无效键、3 单位部分成交、余量 2 查询、再次成交至全满、另一订单部分成交后撤单；Java gRPC 客户端完成下单、查簿、撤单。两次强制结束独立服务进程后，先确认 6 条 V3 Journal 事件恢复为原订单状态，再观察 Snapshot 序号 7 与 Journal 撤单序号 8 的组合；第三次启动同键重试返回原订单 `CANCELED`、剩余 2、无新成交，活动订单簿为空。测试数据留在被忽略的 `target` 目录，原 `data/events` 未改动。Tomcat 对路径段 `%2F` 返回 `400`；使用 `/orders/book?symbol=SMOKE%2FUSDT` 查询参数入口。

## JMH 与手写基准

`MatchingEngineJmhBenchmark` 和手写 benchmark 位于 `src/main/java/com/example/matching/benchmark`。已缓存依赖时，从项目根目录用脚本构造独立 JVM 的完整依赖 classpath：

```powershell
.\scripts\run-jmh.ps1 -SettingsPath 'D:\java\apache-maven-3.9.9\conf\settings.xml'
```

JMH 注解和默认 runner 均使用独立 JVM fork 1 次，默认预热 3×2 秒、测量 5×2 秒。直接用 `mvn exec:java` 启动时，fork 子 JVM 会缺少 JMH classpath；上述脚本先用本地已缓存的 `maven-dependency-plugin:3.8.1` 生成 classpath，再启动 JMH Main。`OrderBook` 场景只测撮合核心，不测 HTTP/gRPC 端到端；Actor 场景也不包含网络协议。性能数据须连同 JVM/硬件、样本规模、成交比例、持久化开关和原始输出一起解释；`mvn test` 不运行 JMH。

P3 已按上述完整 JMH 配置测量三条路径，并用 [`run-p3-probe.ps1`](../scripts/run-p3-probe.ps1) 测逐单响应延迟、Journal 增长、恢复时间及历史容量。复现条件、实测结果、误差和方法边界见 [P3 性能基线](P3_PERFORMANCE.md)。两种性能脚本只写项目 `target/` 下的独立目录；`mvn test` 不运行性能基准。

## 开发测试数据重置

仅对明确指定的项目 `target/` 子目录可使用 `scripts/reset-test-data.ps1 -TestDirectory (Join-Path (Get-Location) 'target/demo-events')`。脚本拒绝默认 `data/events`、项目外路径和目录链接；运行前请自行确认目标确实是可丢弃的实验数据。旧格式 Journal/Snapshot 不会自动转换或删除，启动时将拒绝恢复。
