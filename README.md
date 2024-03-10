## my-redis

`my-redis`是一个用Rust编写的迷你Redis。

Fork自[此仓库](https://github.com/tokio-rs/mini-redis)，但做了些许修改，且具有较为完全的中文注释。

声明：此项目的目的是学习使用`Tokio`编写应用。

### 支持的命令

`my-redis`目前支持的命令如下：

1. `Ping [<message>]`
2. `Set <key> <value> [<expiration(ms)>]`
3. `Get <key>`
4. `Publish <channel> <message>`
5. `Subscribe <channel> [<channel> ...]`

### 命令使用

首先开启服务器：

```bash
cargo run --bin my-redis-server
```

1. 运行`Ping`：

   ```bash
   cargo run --bin my-redis-cli ping hello-server
   ```

2. 运行`Get`和`Set`：

   ```bash
   cargo run --bin my-redis-cli set foo bar
   
   cargo run --bin my-redis-cli get foo
   ```

3. 运行`Subscribe`和`Publish`：

   ```bash
   cargo run --bin my-redis-cli subscribe c
   
   cargo run --bin my-redis-cli publish c 100
   ```

### Tokio模式

这个学习项目应用了很多有用的模式，包括：

#### TCP服务器

`server.rs`启动了一个TCP服务器来接收连接，并为每一个连接开启一个异步任务来处理。它优雅地处理连接时可能发生的错误。

#### Socket之间状态共享

服务器维护一个`Db`实例，所有连接都可以访问该实例。`Db`实例管理键值状态以及发布/订阅功能。

#### Frame

`connection.rs`和`frame.rs`展示了如何理想地实现一个网络协议。该协议使用中间表示形式`Frame`结构建模。`Connection`接收一个`TcpStream`，并公开一个发送和接收`Frame`值的 API。

#### 优雅停机

`tokio::signal`用于侦听 SIGINT。一旦收到信号，关机就会开始。服务器停止接受新连接。现有连接会收到关机通知，等待所有执行中的工作完成，然后关闭服务器。

#### 发布/订阅功能

服务器具有非堆成的发布/订阅功能。客户端可以订阅一个或多个频道，此时客户端处于订阅状态，等待接收信息，无法执行除关闭客户端（Ctrl + C）外的其他活动。服务器使用广播信道和每个连接一个`StreamMap`来实现此功能。客户端可以向某个频道发布信息，其他订阅了此频道的客户端就可以收到这些信息。

#### 使用互斥锁保证数据安全

服务器使用`std::sync::Mutex`确保在并发环境下的数据安全。

### 其他

#### 帮助

可能对项目理解有帮助的架构图：

![miniredis_01](media\miniredis_01.jpg)

___

![miniredis_02](media\miniredis_02.jpg)

#### 未完成的

1. 本项目没有提供单元测试和集成测试，如果有需要可以查看[原仓库](https://github.com/tokio-rs/mini-redis)的`tests`文件夹。

2. 处于订阅状态的客户端无法进行除了退出`Ctrl + C`以外的任何操作，无法重新订阅、取消订阅等操作。