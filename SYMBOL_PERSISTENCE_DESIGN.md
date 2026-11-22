# Symbol Persistence Design

> 历史设计提案：本文中的数据库、Redis、symbol 注册表、多实例所有权、管理流程均**未在当前仓库实现**。现行实现是单机本地文件持久化；运行方式、故障语义和限制以 [README](README.md) 与 [架构说明](docs/ARCHITECTURE.md) 为准。

## 目标

这份文档说明两个问题：

- `symbol` 在持久化层应该怎么建模
- 服务运行中动态新增交易对应该怎么处理

当前项目是单机 `Spring Boot + Akka Typed` 撮合原型。每个交易对对应一个 `SymbolActor`，订单簿在内存里串行撮合。后续如果要把持久化从本地文件升级到 `Redis` 或数据库，核心不是“换个存储介质”这么简单，而是先把 `symbol` 的业务语义和系统内部 key 分开。

## 设计原则

### 1. 原始 symbol 保留为业务字段

业务层继续使用原始交易对，例如：

- `BTC/USDT`
- `ETH/USDT`
- `BTC/USDC`

原因：

- 可读性最好
- 和外部 API、模拟盘、风控、报表一致
- 不需要在业务代码里到处做反向转换

结论：

- 对外接口传原始 `symbol`
- 订单、事件、快照、交易记录里保留原始 `symbol`

### 2. 内部安全 key 单独派生

有些地方不适合直接使用原始 `symbol`：

- Actor 名称
- 本地文件名
- 某些缓存 key

所以内部单独派生一个 `symbol_key`，例如：

- `BTC/USDT -> btc_usdt-xxxx`
- `ETH/USDT -> eth_usdt-xxxx`

这个 key 的职责只有两个：

- 生成安全的内部名称
- 避免特殊字符破坏路径或命名规则

结论：

- 原始 `symbol` 不变
- `symbol_key` 只用于系统内部
- 不要让 `symbol_key` 反过来替代业务字段

## 推荐的数据建模

如果系统要继续扩展，建议引入交易对注册表，而不是让任何字符串都能直接下单。

### 1. symbol_def

建议有一张交易对定义表，至少包含：

- `symbol_id`
- `symbol`
- `base_asset`
- `quote_asset`
- `status`
- `created_at`
- `updated_at`

示例：

| symbol_id | symbol   | base_asset | quote_asset | status |
| --- | --- | --- | --- | --- |
| 1 | BTC/USDT | BTC | USDT | ACTIVE |
| 2 | ETH/USDT | ETH | USDT | ACTIVE |
| 3 | SOL/USDT | SOL | USDT | ACTIVE |

其中：

- `symbol` 是业务展示值
- `symbol_id` 是内部稳定主键
- `status` 用于控制是否允许下单

### 2. 业务数据表

后续订单、事件、快照可以优先围绕 `symbol_id` 做关联，必要时同时保留 `symbol` 方便查询和审计。

例如：

#### order_event

- `id`
- `symbol_id`
- `symbol`
- `sequence`
- `event_type`
- `payload`
- `created_at`

#### order_book_snapshot

- `id`
- `symbol_id`
- `symbol`
- `snapshot_version`
- `snapshot_data`
- `created_at`

这样做的好处：

- 索引更短
- 分区更方便
- 未来改展示名时不影响内部主键

## Redis 方案

## 适用场景

Redis 更适合：

- 热快照缓存
- 高频读场景
- 恢复加速
- 临时事件流缓冲

不建议一开始把 Redis 当作唯一事实存储。

原因：

- AOF/RDB 仍然有丢失窗口
- 主从切换和一致性治理更复杂
- 事件审计和长期归档不如数据库自然

## key 设计

Redis key 可以直接包含原始 `symbol`，例如：

- `matching:snapshot:BTC/USDT`
- `matching:events:BTC/USDT`
- `matching:orders:BTC/USDT:O123`

技术上是可行的，但为了统一风格，也可以改成：

- `matching:snapshot:btc_usdt`
- `matching:events:btc_usdt`

建议：

- Redis key 可以用编码后的 `symbol_key`
- value 或 payload 里保留原始 `symbol`

## 推荐角色

如果接 Redis，建议角色是：

- 最新订单簿快照缓存
- 最近事件缓存
- 恢复加速层

不建议角色是：

- 唯一持久化存储

## 数据库方案

数据库更适合作为事实存储层。

推荐原因：

- 事件审计清晰
- 事务和一致性语义更完整
- 快照和历史归档更容易治理
- 运行时新增交易对更容易纳入注册表管理

### 推荐落地方式

1. `symbol_def` 管理交易对元数据
2. `order_event` 保存增量事件
3. `order_book_snapshot` 保存快照
4. 启动恢复时：
   - 先读最新快照
   - 再回放快照后的事件

### symbol 字段建议

数据库里建议：

- `symbol` 直接存原始值，如 `BTC/USDT`
- `symbol_id` 作为内部索引和关联字段

不建议只存 `btc_usdt` 这种派生 key。那会让数据表失去业务可读性。

## 当前项目推荐的演进方向

按工程优先级看，建议是：

1. 当前文件持久化继续保留，用于 demo 和本地验证
2. 真正要升级时，优先接数据库
3. Redis 作为缓存或恢复加速层，而不是唯一持久化

原因很直接：

- 数据库更适合事件日志和快照治理
- Redis 更适合做性能层，不适合先承担全部账本责任

## 运行时新增交易对

运行中新增交易对，不应该只是“突然来一个新 symbol 就自动建 actor”。更稳的做法是分两步：

### 第一步：注册交易对

把新交易对写入 `symbol_def`，状态初始可以是：

- `PENDING`
- `ACTIVE`
- `DISABLED`

建议规则：

- 只有 `ACTIVE` 状态允许下单
- 未注册 symbol 直接拒绝

### 第二步：创建或预热 SymbolActor

有两种方式：

#### 方式 A：懒创建

- 第一次收到某个已注册 `symbol` 的请求
- `EngineRootActor` 发现本地还没有对应 actor
- 立即创建 `SymbolActor`

优点：

- 实现简单
- 不占多余资源

缺点：

- 首次请求会承担创建和恢复开销

#### 方式 B：主动预热

- 新交易对注册成功
- 后台触发一个“预热 symbol”命令
- 应用先把对应 `SymbolActor` 创建出来

优点：

- 首笔请求延迟更稳

缺点：

- 需要多一条注册到预热的流程

## 当前项目推荐做法

单机原型里建议：

1. 保留现在的懒创建
2. 同时支持配置预热常用交易对
3. 后续再补 `symbol registry`

这样能先把系统边界收紧，不必一次把后台管理全做完。

## 动态新增交易对的建议流程

### 管理流程

1. 管理端创建新交易对，例如 `DOGE/USDT`
2. 写入 `symbol_def`
3. 状态设为 `ACTIVE`
4. 通知撮合服务刷新可用交易对
5. 由服务懒创建或主动预热对应 `SymbolActor`

### 下单流程

1. 用户请求 `PlaceOrder(symbol=DOGE/USDT, ...)`
2. 应用先校验 `symbol_def`
3. 如果未注册或非 `ACTIVE`，直接拒绝
4. 如果合法，路由到 `EngineRootActor`
5. `EngineRootActor` 找到或创建对应 `SymbolActor`
6. `SymbolActor` 加载快照和事件后进入正常撮合

## 多实例服务时的处理

当前项目是单机设计，`symbol -> actor` 的映射保存在本机内存。

如果未来部署成多实例，需要再解决一个关键问题：

- 同一个 `symbol` 只能被一个撮合实例拥有

否则会出现：

- 两台机器同时撮合同一个交易对
- 状态分裂
- 持久化冲突

### 常见方案

#### 1. 静态分片

例如：

- `symbol_id % N`

优点：

- 简单

缺点：

- 扩缩容麻烦

#### 2. 路由层一致性哈希

请求先由网关按 `symbol` 路由到固定实例。

优点：

- 比较实用

缺点：

- 网关和应用都要感知分片规则

#### 3. 集群分片

例如基于集群 sharding 做 symbol ownership。

优点：

- 更完整

缺点：

- 架构复杂度显著上升

## 对当前项目的具体建议

如果目标还是“单机可运行原型”，建议下一阶段按这个顺序推进：

1. 增加 `symbol registry`
2. 下单前校验 `symbol` 是否已注册且 `ACTIVE`
3. 保持原始 `symbol` 作为业务字段
4. 继续用 `symbol_key` 解决 actor 名称和文件命名问题
5. 后续如果要升级持久化，优先接数据库
6. Redis 作为缓存和恢复加速层再接入

## 结论

这个问题的关键不是“`symbol` 用不用斜杠”，而是要明确分层：

- 业务层：保留原始 `symbol`
- 系统内部命名层：派生 `symbol_key`
- 注册管理层：引入 `symbol_def`
- 持久化层：优先数据库，Redis 做辅助

运行中新增交易对也不应该只是简单建 actor，而是：

- 先注册
- 再校验
- 再懒创建或预热

这样后续无论持久化放文件、Redis 还是数据库，模型都不会乱。
