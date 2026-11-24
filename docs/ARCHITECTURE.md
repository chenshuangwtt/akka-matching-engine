# 架构与恢复语义

## 范围

本项目是单机 Java 21 撮合实验：Spring Boot 暴露 HTTP，gRPC Java 暴露同一业务服务；Akka Typed 管理按交易对划分的 Actor。没有账户认证、余额、清结算、跨进程分片或外部事件总线。一个交易对的数据目录不能同时由两个服务进程写入。

## 消息链路

```text
HTTP OrderController ──┐
                       ├─→ OrderApplicationService ─→ EngineRootActor
gRPC OrderGrpcService ──┘                              │ 按原始 symbol 路由
                                                       ↓
                                                   SymbolActor
                                                    │       │
                                        内存 OrderBook       PersistenceActor
                                                              │
                                                       OrderEventStore
                                                       Journal / Snapshot
```

`EngineRootActor` 根据 `symbol` 创建或复用一个 `SymbolActor`，并监听子 Actor 停止，移除失效缓存引用。`matching.preload-symbols` 只预热常用交易对；未列出的交易对收到请求时也会懒创建。原始 `symbol` 保留为业务字段，安全的 Actor/文件 stem 由 `SymbolKeyUtil` 派生。交易对没有注册表。

HTTP 订单簿推荐 `/orders/book?symbol=BTC%2FUSDT` 查询参数；原有 `/orders/book/{symbol}` 只用于不含斜杠的符号。Tomcat 对路径段中的 `%2F` 返回 `400`；查询参数可避开该问题。

`SymbolActor` 的主要状态如下：

1. **loading**：异步请求 `PersistenceActor` 恢复；新业务请求暂存。无历史数据的新交易对可从空订单簿开始。
2. **running**：检查新订单有效性、客户端幂等键和服务端订单 ID。需要持久化的操作发送带请求 ID 的 `AppendPlace`/`AppendCancel`。
3. **awaiting**：等待匹配请求 ID 的写入结果，其他同交易对消息暂存。只有收到 `WriteSucceeded` 才修改 `OrderBook` 并回复业务成功。重复或迟到的非匹配结果不构成成功确认。
4. **faulted**：恢复错误、写入失败或超时、持久化 Actor 停止后拒绝下单与撤单并记录原因。写入边界不明确时，回复 `outcome unknown`，由客户端保留原键重试。

`PersistenceActor` 在独立 dispatcher 上顺序操作本地文件。单交易对请求在 `SymbolActor` 中串行，磁盘 I/O 不阻塞撮合 Actor 的 dispatcher；等待队列有容量和超时边界。Actor 意外重建后先恢复再接单，不能用新空订单簿代替损坏的历史状态。

HTTP/gRPC 共用 `OrderApplicationService` 的非阻塞全局准入计数，默认最多 1000 个在途请求。超额请求在路由前返回不可用，并记录过载日志；已路由而超时的请求仍是结果未知。生产根 Actor 使用容量 5000 的有界 Mailbox，`SymbolActor` 的 Stash 和持久化 Actor 的 Mailbox 也有界。内部代码若绕过应用服务直接投递 ActorRef，需要自行承担同等的准入约束。

## 订单簿和身份

`OrderBook` 用买盘降价、卖盘升价的价格档索引；同价档内按入簿顺序匹配。成交量为双方剩余量的较小值；部分成交订单保留剩余量，完全成交订单移出活动簿。撤单根据活动订单 ID 找节点并移除，只能撤销尚未成交的剩余量；重复撤单不会再次释放数量。

服务端 `orderId` 是已接受订单的身份，不能复用。新订单必须有非空 `userId` 和 `clientOrderId`；幂等键作用域为 `(symbol, 请求中的 userId, clientOrderId)`。同键同方向、价格、数量的请求返回原订单当前状态；参数不同会冲突。`OrderBook` 保留所有已接受订单的身份与状态，而不是只检查活动索引。因此已完全成交或撤销的订单仍可去重。当前 `userId` 未绑定认证账户。历史索引默认每交易对最多 100000 单；到达上限时先拒绝新单，不写 Journal，旧键仍可重试。

首次下单响应包含本次 `Trade` 列表；同键重试返回当前订单状态且不重新产生交易，**不会重放原成交列表**。调用方不能把重试时的空成交列表解释为从未成交。

## Journal 与 Snapshot

每交易对持久化文件为 `<stem>.events.jsonl` 和 `<stem>.snapshot.json`：

- Journal 记录 `PLACE`、`CANCEL`，每条事件有 `formatVersion=3` 和单调序号；`PLACE` 必须携带 `userId`、`clientOrderId`、`orderId`、交易对、方向、价格和数量。
- `Append*` 写完后执行 `FileChannel.force(true)`，再发送成功确认；入队不代表落盘。
- Snapshot 记录 `formatVersion=3`、`lastAppliedSequence`、活动订单顺序 `openOrders`，以及所有已接受订单的 `allOrders` 身份与状态索引。
- 默认达到 1000 个待处理事件且活动订单数不超过 64 时生成检查点；活动订单较多时最迟在 4000 个待处理事件处生成检查点。空订单簿不会绕过事件阈值反复压缩。
- 压缩先在目标旁写临时 Snapshot，刷盘、校验、使用 `ATOMIC_MOVE` 替换；然后才清理 Journal。若文件系统不支持原子替换，则报错而不降级为非原子替换。
- 恢复先读取 Snapshot，再验证 Journal 序号，跳过不大于快照位置的事件。缺失、损坏、残缺、重复或断序导致恢复失败并拒绝撮合。

Windows Java NIO 可 `force` 文件内容，但没有可移植的目录元数据 `fsync`。进程强制退出测试覆盖了文件写入阶段；**物理断电后目录项持久性仍是未验证边界**。`ATOMIC_MOVE` 和文件占用行为依赖目标文件系统，测试结果不能泛化到所有存储设备。

### 不兼容实验数据

V1/V2 等旧 Journal 或 Snapshot 缺少 `formatVersion=3` 时，不能直接恢复；系统明确报不兼容并拒绝撮合。程序不自动迁移、不删除或清空文件。开发者如要重置实验数据，须显式指定 `target/` 下的独立测试目录并运行 `scripts/reset-test-data.ps1 -TestDirectory <path>`；默认 `data/events` 不在允许范围。

## 故障处理与边界

响应超时、Actor 退出或网络断开时，客户端不知道第一次请求是否已持久化。它应保留同一个 `clientOrderId` 和原参数，待交易对恢复后重试；已接受订单不会再执行一次。写入失败后的未完成撤单也可能未知；重复撤单仅报告当前是否仍为活动订单，并非历史撤单收据。

Snapshot 只保存撮合状态，不保存完整成交明细；Journal 压缩后也不能提供历史成交查询。持久化路径的成交 ID 采用 `T:<交易对 UTF-8 Base64URL>:<PLACE 事件序号>:<该事件内成交序号>`，同一 Journal 回放可重建相同 ID，跨交易对不冲突。无持久化的内存基准使用带 `U:` 前缀的临时随机 ID，不提供重启语义。当前没有不可变成交事实、资金对账或清结算能力。历史身份索引的内存和 Snapshot 存储均随订单数量线性增长；每单至少需要一条 `Order` 记录和两个映射条目，实际字节数受 JVM、字符串长度和 JSON 字段长度影响。容量上限保护可用性，但没有安全回收与归档策略。

代码入口：[EngineRootActor](../src/main/java/com/example/matching/actor/EngineRootActor.java)、[SymbolActor](../src/main/java/com/example/matching/actor/SymbolActor.java)、[PersistenceActor](../src/main/java/com/example/matching/actor/PersistenceActor.java)、[OrderBook](../src/main/java/com/example/matching/engine/OrderBook.java)、[OrderEventStore](../src/main/java/com/example/matching/persistence/OrderEventStore.java)。
