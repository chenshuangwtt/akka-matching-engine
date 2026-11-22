# akka-matching-engine

基于 **Java 21、Spring Boot 4、Akka Typed** 的单机限价单撮合实验项目。每个交易对由一个 `SymbolActor` 串行处理，订单簿按价格优先、同价时间优先撮合；本地 Journal 与 Snapshot 用于重启恢复。项目重点是验证写入确认、故障关闭、进程崩溃恢复和客户端安全重试，而非提供完整交易所系统。

## 技术栈与结构

| 组件 | 职责 |
| --- | --- |
| Spring Boot 4 / HTTP | 接收下单、撤单、订单簿请求；默认端口 `8080` |
| gRPC Java / Protobuf | 同一应用服务的另一入口；默认端口 `50051` |
| `EngineRootActor` | 按原始 `symbol` 路由，懒创建或预热 `SymbolActor`，清理已停止的引用 |
| `SymbolActor` | 单交易对顺序处理、幂等检查、等待 Journal 确认、恢复与故障状态 |
| `PersistenceActor` / `OrderEventStore` | 顺序写 Journal、刷盘、生成 Snapshot、加载恢复数据 |
| `OrderBook` | 内存价格时间优先撮合、部分成交与撤单 |

消息主线：`HTTP/gRPC → OrderApplicationService → EngineRootActor → SymbolActor`。`SymbolActor` 向 `PersistenceActor → Journal/Snapshot` 请求持久化确认，确认后才操作内存 `OrderBook` 并回复业务成功；等待期间同交易对消息暂存，Actor dispatcher 不做同步磁盘等待。详细状态和文件格式见 [架构说明](docs/ARCHITECTURE.md)。

## 快速开始

- JDK **21**，系统安装的 Maven **3.9.x**。先检查 `java -version`、`javac -version`、`mvn -version`。
- 构建需要 Maven 可解析 `pom.xml` 中的依赖和 Protobuf 生成插件。首次在新机器上运行时，应使用自己配置的 Maven 仓库；离线模式仅在依赖已缓存时可用。
- 当前 Windows 验证环境为 Maven `D:\java\apache-maven-3.9.9`、本地仓库 `D:\Maven`。本项目测试和启动均使用系统 Maven。工作目录中遗留的 Maven Wrapper 脚本指向另一 Maven 版本且可能下载文件；原文件保留供本地追溯，已从待发布文件排除，**未使用或验证 Maven Wrapper**。

在本机已缓存依赖的环境中：

```powershell
mvn -o -s "D:\java\apache-maven-3.9.9\conf\settings.xml" test
mvn -o -s "D:\java\apache-maven-3.9.9\conf\settings.xml" spring-boot:run
```

其他机器按自身 Maven 设置执行 `mvn test` 和 `mvn spring-boot:run`。默认开启持久化，数据写入 `data/events`；首次试用请使用独立空目录，不要指向已有撮合数据。配置位于 [`application.properties`](src/main/resources/application.properties)：

| 配置项 | 默认值 | 作用 |
| --- | --- | --- |
| `server.port` | Spring Boot 默认 `8080` | HTTP 端口 |
| `grpc.server.port` | `50051` | gRPC 端口 |
| `matching.persistence.enabled` | `true` | 启用本地持久化 |
| `matching.persistence.dir` | `data/events` | Journal / Snapshot 目录 |
| `matching.persistence.snapshot-threshold` | `1000` | 快照事件阈值之一 |
| `matching.persistence.compact-low-load-open-orders` | `64` | 低活动订单数条件 |
| `matching.max-historical-orders-per-symbol` | `100000` | 每交易对历史幂等索引容量；满后拒绝新单，保留旧键供重试 |
| `matching.max-in-flight-requests` | `1000` | HTTP/gRPC 共用在途请求准入上限；超额立即返回不可用 |
| `matching.preload-symbols` | `BTC/USDT,ETH/USDT,SOL/USDT,BTC/USDC,ETH/USDC` | 启动时预热交易对；其他交易对按请求懒创建 |

Spring Boot 命令行参数可覆盖配置，例如在**新的空目录**中试用：

```powershell
mvn -o -s "D:\java\apache-maven-3.9.9\conf\settings.xml" spring-boot:run "-Dspring-boot.run.arguments=--matching.persistence.dir=target/demo-events --matching.preload-symbols="
```

启动后 HTTP 为 `http://localhost:8080`，gRPC 为 `localhost:50051`。停止应用用 `Ctrl+C`。`target/demo-events` 是示例数据目录，构建输出会被 `.gitignore` 排除；不要将它当作长期存储。

## 撮合与幂等行为

买单先匹配最低可成交卖价，卖单先匹配最高可成交买价；同一价格档按入簿顺序成交。部分成交后仅剩余数量留在订单簿；撤单只移除尚未成交的剩余数量。`price`、`quantity` 均为整数，项目未定义币种精度、金额单位或账户余额规则。

服务端生成 `orderId`；客户端**必须**提供非空 `clientOrderId`。有效幂等键为 **`(symbol, 请求中的 userId, clientOrderId)`**。同键同方向、价格和数量的重试返回原 `orderId` 与当前订单状态，不再次撮合；同键参数变化返回冲突。已完全成交或撤销的订单身份仍保留在恢复数据中。当前 `userId` 没有身份认证，调用方需自行保证其可信度。价格和数量是正整数，分别不得超过 `1000000000`；本项目未定义交易品种的精度或真实金额单位。

若请求超时或返回 `outcome unknown`，保留原 `clientOrderId` 和所有订单参数，服务可用后原样重试：已落盘则关联原订单，未落盘则可能成为首次有效请求。不要将未知结果直接当成失败，也不要换键重试。重试只保证业务幂等，不保证端到端 Exactly Once。gRPC 下单响应只有 `order_id` 和文本 `message`，当前状态与剩余量在重试消息中；HTTP 响应包含 `finalOrder`。没有独立的历史订单查询接口。

## HTTP 示例

以下 PowerShell 示例使用同一 `clientOrderId` 安全重试。为方便观察，建议先用上面的空目录启动：

```powershell
$body = @{userId='u1';clientOrderId='buy-001';symbol='BTCUSDT';side='BUY';price=10000;quantity=5} | ConvertTo-Json
$placed = Invoke-RestMethod -Uri 'http://localhost:8080/orders/place' -Method Post -ContentType 'application/json' -Body $body
$placed.data.finalOrder.orderId
$retry = Invoke-RestMethod -Uri 'http://localhost:8080/orders/place' -Method Post -ContentType 'application/json' -Body $body
$retry.data.finalOrder.orderId
```

以另一个 `userId` 提交可成交的 3 单位卖单，再查询和撤销买单剩余量：

```powershell
$sell = @{userId='u2';clientOrderId='sell-001';symbol='BTCUSDT';side='SELL';price=10000;quantity=3} | ConvertTo-Json
Invoke-RestMethod -Uri 'http://localhost:8080/orders/place' -Method Post -ContentType 'application/json' -Body $sell
Invoke-RestMethod -Uri 'http://localhost:8080/orders/book?symbol=BTCUSDT'
$cancel = @{symbol='BTCUSDT';orderId=$placed.data.finalOrder.orderId} | ConvertTo-Json
Invoke-RestMethod -Uri 'http://localhost:8080/orders/cancel' -Method Post -ContentType 'application/json' -Body $cancel
```

同键不同参数返回 HTTP `409`；无效订单返回 `400`；恢复、写入故障或过载返回 `503`。可编辑的请求文件见 [`akka-test.http`](akka-test.http)。

HTTP 订单簿推荐 `GET /orders/book?symbol=BTC%2FUSDT`，可查询带斜杠的交易对。原有 `/orders/book/{symbol}` 仍可用于不带斜杠的符号；不要把 `%2F` 放进路径段。下单和撤单在 JSON body 中携带原始 `symbol`。

## gRPC 示例

协议见 [`order.proto`](src/main/proto/order.proto)：`OrderService/PlaceOrder`、`CancelOrder`、`GetBook`。`PlaceOrderRequest.client_order_id` 已参与幂等；`username` 非空时用作当前 `userId`，否则将数字 `user_id` 转成字符串。冲突为 `ALREADY_EXISTS`，无效订单为 `INVALID_ARGUMENT`，不可用或未知结果为 `UNAVAILABLE`。

先启动服务，再在另一个终端运行仓库中的 Java 客户端示例（默认连接 `localhost:50051`）：

```powershell
mvn -o -s "D:\java\apache-maven-3.9.9\conf\settings.xml" compile exec:java "-Dexec.mainClass=com.example.matching.grpc.OrderGrpcClientExample"
```

客户端也可传入主机和端口：`"-Dexec.args=localhost 50051"`。跨项目生成 Java stub 的说明见 [GRPC_STUB_USAGE.md](GRPC_STUB_USAGE.md)；生成物不纳入版本库。

## 持久化、恢复与故障测试

Journal 是每交易对一个带单调序号的 JSON Lines 文件，记录 `PLACE` 和 `CANCEL`。Snapshot 记录最后应用的序号、活动订单和已接受订单身份。写入确认对应 Journal 文件 `FileChannel.force(true)` 完成，而非消息入队。压缩时先写并校验同目录临时 Snapshot，再原子替换；只有之后才清理 Journal。恢复时跳过快照已包含的事件，并检查尾部残缺、重复或断序。恢复失败进入故障状态，拒绝下单和撤单，不自动清空历史。

测试包含五个独立 JVM 强制退出点，以及 Actor 重启、持久化 Actor 故障、Journal/Snapshot 损坏、幂等重试和受控过载。故障注入使用 JUnit 临时目录与同步标记，不操作默认 `data/events`。运行方式和场景列表见 [测试说明](docs/TESTING.md)。截至 **2026-11-22**，本机离线 `mvn test` 为 **48/48 通过**；这是测试结果，不是物理断电保证。

## 性能测试

项目含 JMH 和有界独立负载探针。以下为 **2026-11-22 本机独立负载探针**的计时结果：预热 100 单、测量 5000 单，Actor 路径最多 16 个在途请求，同价买单挂簿、无交叉成交。

| 测量路径 | TPS | p50 / p95 / p99 |
| --- | ---: | --- |
| 直接 `OrderBook` | 199,246 | 0.0027 / 0.0104 / 0.0264 ms |
| Actor 业务回复、无持久化 | 21,499 | 0.448 / 1.892 / 3.720 ms |
| Actor 业务回复、Journal 写盘确认 | 808 | 18.328 / 23.511 / 31.276 ms |

环境为 Windows 10、Ryzen 7 3700X、约 32 GiB 内存、Zulu JDK 21.0.9、Maven 3.9.9。直接订单簿的计时窗口很短，结果易受运行时噪声影响。**这三条路径的工作量不同，均不是 HTTP/gRPC 端到端 TPS。** JMH 另以每批 1000 单报告 `ops/s`，不可将其单位与表中 TPS 混用。参数、原始输出名称、误差、复现脚本和局限见 [P3 性能基线](docs/P3_PERFORMANCE.md)。

## 功能边界与许可

- 单机单进程内按交易对串行撮合；未实现多实例所有权、身份认证、余额冻结或清结算。
- Journal 和 Snapshot 只接受 `formatVersion=3`；旧实验格式会明确拒绝恢复，不迁移、不清空。开发数据重置脚本 [`scripts/reset-test-data.ps1`](scripts/reset-test-data.ps1) 只允许显式指定项目 `target/` 下的测试目录，不能用于默认 `data/events`。
- Snapshot 不保存完整成交历史；成交 ID 由交易对、已持久化的 `PLACE` 事件序号和事件内成交序号确定，重启后不会复用，但不能据此做历史成交查询或资金对账。
- 历史幂等索引随已接受订单线性增长。默认每交易对上限 100000 单，满后返回不可用并保留旧记录；没有安全回收或归档。Windows 目录元数据刷盘与物理断电持久性未获得验证。
- 不宣称严格的端到端 Exactly Once，也不宣称具备生产级交易或清结算能力。

本仓库自有代码采用 [MIT License](LICENSE)。Akka 2.8.8 等第三方依赖适用各自许可证；特别是 Akka 2.8.8 的 **BSL 1.1** 对生产使用有限制，项目 MIT 不覆盖它。见 [第三方许可说明](docs/THIRD_PARTY_LICENSES.md)。
