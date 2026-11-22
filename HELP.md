# 开发入口

本文件保留为旧链接的入口。当前项目说明以 [README.md](README.md) 为准：

- [架构、撮合与恢复](docs/ARCHITECTURE.md)
- [离线测试、故障注入与 JMH 使用边界](docs/TESTING.md)
- [自有代码和第三方许可](docs/THIRD_PARTY_LICENSES.md)
- [gRPC 客户端交付](GRPC_CLIENT_DELIVERY.md)及 [Java stub 生成](GRPC_STUB_USAGE.md)
- [未来 symbol 注册与存储选型讨论](SYMBOL_PERSISTENCE_DESIGN.md)（设计提案，尚未实现）

实际默认端口为 HTTP `8080`、gRPC `50051`。恢复失败会拒绝撮合，不会清空历史订单。完整离线测试命令和已验证结果见 README 与测试说明。
