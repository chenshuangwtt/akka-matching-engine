# P3 有界负载与性能基线

## 环境和边界

2026-11-22，Windows 10，AMD Ryzen 7 3700X（8 核/16 逻辑处理器），物理内存 34,250,878,976 B（约 31.9 GiB）。Zulu JDK 21.0.9，Maven 3.9.9，既有 `D:\Maven` 仓库，JMH 1.37。Java 进程报告最大堆 8,564,768,768 B。数据均写到项目 `target/` 下独立目录；未改动 `data/events`。以下是单机样本，不代表其他磁盘、JVM 或生产负载。

## JMH 批次吞吐

运行 [`scripts/run-jmh.ps1`](../scripts/run-jmh.ps1)，每次独立 fork 1 个 JVM，预热 3×2 秒、测量 5×2 秒。参数统一为每批 1000 单、100 个价格档、`cross=true`。下表 `ops/s` 的一个操作是一批 1000 单；“折算订单/s”只用于说明批次规模，不是 HTTP/gRPC TPS。

| 场景 | 执行路径 | 批次 ops/s | 折算订单/s | 原始记录 |
| --- | --- | ---: | ---: | --- |
| `orderBook` | 直接 `OrderBook.place`，含 MatchResult/Trade 分配 | 1613.537 ± 39.296 | 1,613,537 | `target/p3-jmh-orderbook-final.log` |
| `singleSymbolActor` | `PlaceOrderNoReply`，无 Journal、无业务回复 | 781.270 ± 84.013 | 781,270 | `target/p3-jmh-actor-final.log` |
| `singleSymbolActorWithPersistence` | Actor 撮合与 Journal force 确认，回复发给 ignoreRef | 0.827 ± 0.091 | 827 | `target/p3-jmh-persistence-final.log` |

上述误差为 JMH 输出；这些场景不测 HTTP/gRPC。持久化 JMH 等待已处理计数，包含逐笔 Journal 确认，但不保证每次 Invocation 的异步 Snapshot 清理都计入测量时间。修复前相同持久化场景的**短测**（预热 1×1 秒、测量 2×1 秒）为 0.138 批/s，修复后同配置为 0.786 批/s；短测仅用于定位过频 Snapshot，不作为正式性能基线。

复现命令示例：

```powershell
.\scripts\run-jmh.ps1 -SettingsPath 'D:\java\apache-maven-3.9.9\conf\settings.xml' -JmhArgs @('MatchingEngineJmhBenchmark.orderBook$','-wi','3','-i','5','-f','1','-p','orders=1000','-p','priceLevels=100','-p','cross=true')
```

将场景名依次改为 `MatchingEngineJmhBenchmark.singleSymbolActor$`、`MatchingEngineJmhBenchmark.singleSymbolActorWithPersistence$`。脚本通过已缓存的 Maven 插件生成完整 classpath；直接用 `mvn exec:java` 启动 forked JMH 会缺少子进程 classpath。

## 独立负载探针：逐单响应与延迟

运行 [`scripts/run-p3-probe.ps1`](../scripts/run-p3-probe.ps1)，预热 100 单、测量 5000 单、最多 16 个在途请求，单交易对、同价买单挂簿；订单无交叉成交。延迟从提交到本地 Actor 业务响应，包含排队，按完成时间统计分位数。该探针不经过 HTTP/gRPC。与上方 `cross=true` JMH 场景的订单分布、结果分配和 dispatcher 条件不同，不能直接用两表相除推断框架开销。

测量保留项目默认 INFO 日志；日志写入和控制台重定向也会影响观察值。TPS 是这一负载窗口的完成量除以耗时，延迟分位数是单次响应样本，不是网络端到端指标。

| 路径 | TPS | p50 | p95 | p99 | 记录 |
| --- | ---: | ---: | ---: | ---: | --- |
| 直接 OrderBook | 199,246.05 | 0.002700 ms | 0.010400 ms | 0.026399 ms | `target/p3-probe-final2.log` |
| Actor 业务回复，无持久化 | 21,498.82 | 0.448 ms | 1.892 ms | 3.720 ms | 同上 |
| Actor 业务回复，Journal force | 808.34 | 18.328 ms | 23.511 ms | 31.276 ms | 同上 |

直接 OrderBook 的计时窗口仅约 25 ms，分位数和 TPS 容易受 JIT、GC 和时钟噪声影响；JMH 批次结果更适合观察撮合核心吞吐。持久化路径是每笔同步 `FileChannel.force(true)` 的真实确认边界，不能用纯 OrderBook 数字替代。

复现命令：

```powershell
.\scripts\run-p3-probe.ps1 -SettingsPath 'D:\java\apache-maven-3.9.9\conf\settings.xml' -Measured 5000 -Warmup 100 -OrdersPerSymbol 1000 -Symbols 4
```

本次 5100 笔确认事件的恢复位置为 5100；活动订单多于 64 时，在第 4000 条事件生成 Snapshot。最终 Snapshot 1,521,443 B，Journal 224,400 B，保留 1100 条待回放事件；加载恢复状态约 146.042 ms。此前 500 条、尚未触发压缩的样本 Journal 为 99,564 B，约 199 B/事件。Journal 增长取决于订单字段长度和取消比例。

## 历史身份容量样本

探针调用 `System.gc()` 后读取 `MemoryMXBean` 已用堆；这是近似的**保留堆差值**，不包含序列化时的峰值、Actor/线程、磁盘缓存和 JVM 启动成本。Snapshot 大小是同一 V3 结构的**合成序列化样本**，用于容量估算，不作为可单独恢复的数据集。

| 样本 | 保留堆增量 | 合成 Snapshot 合计 | 约每单 Snapshot |
| --- | ---: | ---: | ---: |
| 4 个交易对 × 2500 单 | 4,230,568 B | 4,182,652 B | 418.3 B |
| 1 个交易对 × 100000 单（默认上限） | 41,775,896 B | 42,355,671 B | 423.6 B |

100000 单样本确认下一笔被 `OrderBook` 容量上限拒绝；Actor 层另有写 Journal 前拒绝和重启后幂等重试测试。4 个交易对样本只测订单簿及历史索引，不包括 4 个 Actor 的完整运行开销。若高频压缩大 Snapshot，瞬时堆和 I/O 可能高于上述保留值。

## 过载、检查点和可观测性

- HTTP/gRPC 共用的 `OrderApplicationService` 默认最多 1000 个在途请求；`tryAcquire` 立即拒绝超额请求，返回 HTTP 503 / gRPC `UNAVAILABLE`。拒绝发生在路由前，并记录 `Request overload`。已发往 Actor 的超时请求仍标为结果未知；客户端应以原幂等键重试。
- 生产 `EngineRootActor` 使用容量 5000 的有界 Mailbox；`SymbolActor`、`PersistenceActor` 原有专用有界 Mailbox 和 Stash 保留。服务入口限额小于根 Mailbox 容量。内部代码直接持有 ActorRef 并绕过服务准入时，不具备相同的明确过载响应保证。
- 默认 Snapshot 阈值为 1000 个事件；活动订单数不超过 64 时达到阈值压缩，超过 64 时最迟在 4000 个待处理事件处压缩。空订单簿不再每次都触发 Snapshot。Journal 仍逐笔 force，未做批量确认或改变 P0 安全语义。
- 恢复失败、Journal 写入失败、请求结果未知、准入过载、Snapshot 提交失败与历史容量触发均有明确日志；目前没有持久化指标时序或外部监控平台。
