# 许可边界与第三方依赖

本仓库的**自有源码与文档**按根目录 [MIT License](../LICENSE) 发布。该 MIT 文本不改变 Akka、Spring Boot、gRPC、JMH、Protobuf 或任何传递依赖的原始许可；依赖仍由各自版权方授权。本仓库不提交 Maven 缓存、依赖 JAR 或生成的二进制发布包。

## Akka 2.8.8

`pom.xml` 直接使用 `akka-actor-typed_2.13:2.8.8` 与 `akka-serialization-jackson_2.13:2.8.8`。本机 Maven POM 对二者均标为 **BUSL-1.1**。该版本的[原始 LICENSE](https://raw.githubusercontent.com/akka/akka/v2.8.8/LICENSE)写明：Licensed Work 为 Akka 2.8.8；标注的 **Change Date 为 2027-10-28**，Change License 为 Apache License 2.0。许可证正文还规定若该版本首次公开分发四周年更早，则以较早日期为准。日期和授权按具体版本判断，不能由本仓库 MIT 替代。

Akka 官方[许可 FAQ](https://akka.io/bsl-license-faq)说明：学习、开发、测试及其他非生产用途可按 BSL 使用；生产使用须符合其授权条件。**2.8.8 原始 LICENSE 中列出的 Additional Use Grant 针对特定 Play Framework 用途，并非本撮合项目的普遍生产授权**。公开 GitHub 源码不自动使部署获得生产许可。计划生产使用者应阅读该版本原始 LICENSE 与官方说明，并确认自身使用权。

## 其他直接依赖与构建工具

以下为当前 `pom.xml` 的主要直接依赖与构建工具，依据本机缓存 POM 或上游原始许可证核对；**不是完整传递依赖清单**。

| 组件 | 当前版本来源 | 上游许可或需遵守的文本 |
| --- | --- | --- |
| Spring Boot starters / parent | parent `4.0.5` | [Apache License 2.0](https://github.com/spring-projects/spring-boot/blob/v4.0.5/LICENSE.txt) |
| gRPC Java | `1.71.0` | [Apache License 2.0](https://github.com/grpc/grpc-java/blob/v1.71.0/LICENSE) |
| Protocol Buffers Java / protoc | `3.25.5` | [上游 BSD 风格三条款许可证](https://raw.githubusercontent.com/protocolbuffers/protobuf/v25.5/LICENSE)，重分发需保留版权与许可说明 |
| Lombok | 由 Spring Boot parent 管理，本机为 `1.18.42` | [MIT 及随包组件说明](https://raw.githubusercontent.com/projectlombok/lombok/v1.18.42/LICENSE) |
| JMH | `1.37` | [上游 GPLv2 + Classpath Exception 文本](https://github.com/openjdk/jmh/blob/master/LICENSE)；应核对具体文件和分发方式 |
| protobuf-maven-plugin、os-maven-plugin、exec-maven-plugin | 版本见 `pom.xml` | 本机缓存 POM 标为 Apache License 2.0 |

本项目此次只发布自有源码与文档，不附带第三方 JAR、Maven 缓存或容器镜像。MIT 授权只覆盖作者有权授权的部分。学习和本地测试仍需遵守各依赖原许可；若未来分发可执行 JAR、容器镜像或在商业生产环境运行，应按实际打包内容生成完整依赖清单，随分发物保留适用的版权、LICENSE 和 NOTICE 文本，核对 Akka 的生产授权，并检查所有传递依赖。这里的核对是源码发布记录，不代表已完成未来二进制分发的逐件法律审查。
