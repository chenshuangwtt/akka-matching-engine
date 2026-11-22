# gRPC Client Delivery

客户端要调用当前项目的 gRPC 服务，实际只需要下面这些内容。

## 必需

### 1. proto 文件

直接给客户端这个文件：

- [order.proto](src/main/proto/order.proto)

客户端拿到 `order.proto` 后，就可以自己生成对应语言的 stub。

### 2. 服务地址

默认地址：

- `127.0.0.1:50051`（可通过 `grpc.server.port` 覆盖）

### 3. 可调用接口

当前暴露：

- `OrderService/PlaceOrder`
- `OrderService/CancelOrder`
- `OrderService/GetBook`

`PlaceOrderRequest.client_order_id` 必须由客户端提供非空值；重试同一订单时保持该值及方向、价格、数量不变。`username` 非空时作为当前声明的 `userId`，否则要求正数 `user_id`。当前服务不验证声明的身份。输入错误返回 `INVALID_ARGUMENT`，同键参数冲突返回 `ALREADY_EXISTS`，服务不可用或结果未知返回 `UNAVAILABLE`。

## 可选

如果客户端也是 Java，不想自己跑 `protoc`，可以额外给它：

- 已生成好的 `com.example.order.grpc` Java stub

但这不是必须项。最标准的交付物仍然是 `order.proto`。

## 结论

对客户端来说，最小交付物就是：

1. `order.proto`
2. gRPC 服务地址
3. 接口说明

不需要把整个服务端工程给客户端。
