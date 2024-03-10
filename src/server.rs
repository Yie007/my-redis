//! my-redis的服务器的实现。
//!
//! 提供了异步的`run()`函数来监听到来的连接并为每个连接生成异步作业。

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
    time,
};

/// Server Listner，包装了`tokio::net::TcpListener`，
/// 在`server::run()`方法内被创建。
///
/// 负责 Tcp 侦听以及连接初始化。
#[derive(Debug)]
struct Listener {
    // `tokio::net::TcpListener`，由`run()`方法提供。
    listener: TcpListener,

    // 数据库`Db`的包装类，负责在被 drop 的时候通知后台工作程序。
    db_holder: DbDropGuard,

    // 信号量，用于限制最大连接数。
    limit_connection: Arc<Semaphore>,

    // 广播发送端，用于通知所有`Handler`停止运行。
    notify_shutdown: broadcast::Sender<()>,

    // mpsc发送端，用于知道何时所有`Handler`都达到了安全状态。
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 连接的操作句柄，每一个 Tcp 连接都对应一个`Handler`。
///
/// 它的任务包括：读取命令、解析命令、执行命令、响应、监听关闭信号等。
#[derive(Debug)]
struct Handler {
    // 共享的数据库操作句柄
    // 当获取到一个`Command`后，它会被`Db`应用。每个命令都会用到它来完成任务。
    db: Db,

    /// TcpStream 的封装，提供了符合 Redis 协议的编码器和解码器
    // 当`Listener`获取到一个到来的连接后，`TcpStream`会被传递给`Connection::new()`
    // `Connection`允许`Handler`在`Frame`层面进行操作，具体的字节层面的协议解析和封装
    // 都被`Connection`封装好了
    connection: Connection,

    // 订阅`Listen`的广播发送端，广播接收端被封装在`Shutdown`中
    // 当接收到关闭信号时，所有正在执行的工作将会继续，直到它们达到安全状态
    shutdown: Shutdown,

    // 当`Handler`离开作用域被丢弃时，这个字段也会被丢弃。
    // 以此表示该`Handler`已完成收尾工作
    _shudown_complete: mpsc::Sender<()>,
}

/// 最大连接数。
const MAX_CONNECTION: usize = 250;

/// 启动 my-redis 服务器。
///
/// 他会将传入的`tokio::net::TcpListener`包装为自定义的`Listener`，
/// 然后同时启动`Listener`以及`shutdown`异步任务，后者用于监听关闭信号。
///
/// 可以使用`tokio::signal::ctrl_c()`作为`shutdown`参数。
///
/// # Errors
/// 如果`Listener`运行出错，返回`Err`。
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 我们只获取广播的发送端，因为可以直接订阅广播发送端。
    // 信道的信息容量设置为1即可，毕竟只需要发送一次信息。
    let (notify_shutdown, _) = broadcast::channel(1);
    // 获取mpsc的发送端和接收端。
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // 创建自定义的 Listner。
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connection: Arc::new(Semaphore::new(MAX_CONNECTION)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // 运行 server 的同时监听关闭信号。
    // server 只有在出现错误的时候才会结束，因此通常情况下下面的语句
    // 会一直运行，直到 shuntdown 这个`Future`运行完成，即接收到关闭信号。
    tokio::select! {
        res = server.run() => {
            // 出错，抛出错误。
            if let Err(err) = res{
                println!("服务器启动失败，原因：{}",err);
            }
        }
        _ = shutdown => {
            println!("接收到关闭信号，准备关闭");
        }
    }

    // 使用模式匹配将两个发送端提取出来。
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // 这里丢弃了广播发送端，广播接收端此时会接收到`None`，
    // 于是它们便可以开始执行清理工作。
    drop(notify_shutdown);

    // 丢弃自己的mpsc发送端，让下面的接收端最终能够接收到`None`。
    drop(shutdown_complete_tx);

    // 等待所有`Handler`完成清理工作，之后所有`Handler`便会因离开作用域而被丢弃，
    // 其内部的`mpsc::Sender`也会被丢弃。
    // 所有的mpsc发送端都被丢弃后，接收端最终返回`None`，服务器关闭。
    let _ = shutdown_complete_rx.recv().await;
    println!("服务器已关闭");
}

impl Listener {
    /// 运行 Server。
    ///
    /// 侦听到达的连接，对于每个到达的连接，都会启动一个异步任务来处理。
    ///
    /// # Errors
    ///
    /// 当接收连接产生错误时，返回`Err`。导致错误的原因有很多，一个常见的
    /// 原因是操作系统开启的 socket 已经达到其限制。
    async fn run(&mut self) -> crate::Result<()> {
        // 这是一个无限循环，除非循环内抛出了错误。
        loop {
            // 等待信号量可用。
            // `acquire_owned()`返回一个 permit，当它被 drop 的时候，信号量会自动递增。
            // `acquire_owned()`返回`Err`的时候表示信号量被关闭了，我们没有关闭信号量，因此`unwrap()`是安全的。
            let permit = self.limit_connection.clone().acquire_owned().await.unwrap();

            // 获取一个新的 socket。由于我们已经在`accept()`内部尝试恢复错误，
            // 所以如果还是抛出了错误，那么这个错误就是不可恢复的。
            // 此时应该退出循环，结束 server。
            let socket = self.accept().await?;

            // 为每个连接都创建一个`Handler`，由`Handler`负责工作。
            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shudown_complete: self.shutdown_complete_tx.clone(),
            };

            // 开启一个异步任务，将`Handler`传入，让其运行。
            tokio::spawn(async move {
                // `Handler`开始工作，处理错误。
                if let Err(err) = handler.run().await {
                    println!("连接错误，原因：{}", err);
                }
                // 工作完成，将 permit 丢弃，信号量递增。
                drop(permit);
            });
        }
    }

    /// 接收一个到来的连接，并尝试处理错误。
    ///
    /// 通过后退和重试来处理错误。使用指数退避策略。第一次失败后，任务将等待1秒。
    /// 第二次失败后，任务将等待2秒钟。每次失败都会使后续的等待时间翻倍。
    ///
    /// # Errors
    ///
    /// 如果在等待64秒后第6次尝试时仍接收失败，返回`Err`。
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        // 尝试接收连接。
        loop {
            // 等待连接到来。如果成功，直接返回，否则尝试重试。
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // 失败太多次了，返回错误。
                        return Err(err.into());
                    }
                }
            }

            // 暂停一段时间。
            time::sleep(Duration::from_secs(backoff)).await;

            // 翻倍等待时间。
            backoff *= 2;
        }
    }
}

impl Handler {
    /// 处理单独的一个连接。当收到关闭信号后，这个函数会运行至安全状态再退出。
    ///
    /// `Handler`的核心函数，负责从 socket 中读取命令、解析命令、
    /// 执行命令、写回响应数据等。
    ///
    /// # Errors
    /// 上述任何一个任务出现错误，返回`Err`。
    async fn run(&mut self) -> crate::Result<()> {
        // 只要`Shuntdown`还未接收到关闭信号后，继续循环。
        while !self.shutdown.is_shutdown() {
            // 启动`Shutdown`的 async 函数，等待接收关闭信号，
            // 同时尝试从`Connection`中读取帧。
            // 只要“读取帧”这个行为先于“接收到关闭信号”，那就往下继续执行。
            let maybe_frame = tokio::select! {
                // 如果读取数据帧出错，抛出错误。
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // 关闭信号被视为是正常的终止，返回的是`Ok`
                    return Ok(())
                }
            };

            // 如果`read_frame()`返回的是`None`，说明对方正常关闭了 socket。
            // 那么我们也不需要继续往下执行了，正常终止即可。
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // 将数据帧转化为`Command`。
            // 如果转化失败，说明为不合法或无法识别的操作命令，抛出错误。
            let cmd = Command::from_frame(frame)?;

            let cmd_name = cmd.get_name().to_string();
            // 执行命令，这有可能会更改数据库的状态。
            // `Handler`的“写回响应数据”的任务也委派给了它，因此传入`Connection`。
            // 如果执行出错，抛出错误。
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
            println!("{cmd_name} finished!");
        }
        // 如果执行到此，说明收到了关闭信号，正常退出循环，返回`Ok`。
        Ok(())
    }
}
