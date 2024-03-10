use tokio::sync::broadcast;

/// 侦听关闭信号。
///
/// `Shutdown`封装了一个`broadcast::Receiver`，当广播的发送端被丢弃的时候，
/// 接收端便会接收到`None`。
#[derive(Debug)]
pub(crate) struct Shutdown {
    // 接收关闭信息。
    notify: broadcast::Receiver<()>,

    // 如果接收到了关闭信号，这个值就为`true`。
    // 因为`Handler`是循环执行，收到关闭信号后要等正在执行的循环结束，
    // 下一次循环开始，然后判断循环条件的时候发现为`false`，退出。
    // 所以要提供布尔表达式作为循环条件，这就是为什么要有这个字段，也是为什么要有这个结构体。
    is_shutdown: bool,
}

impl Shutdown {
    /// 封装广播接收端，返回创建的`Shutdown`。
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            notify,
            is_shutdown: false,
        }
    }

    /// 如果接收到了关闭信号，返回`true`。
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// 等待接收关闭信号。
    /// 如果接收到关闭信号，设置`shutdown`字段为`true`。
    pub(crate) async fn recv(&mut self) {
        // 如果已经接受过关闭信号，直接返回
        if self.is_shutdown {
            return;
        }

        // 当广播发送端被丢弃后，这个 async 函数执行结束，返回`None`。
        // 显然，当且仅当收到关闭信号后，这个函数才会正常执行结束。
        let _ = self.notify.recv().await;

        self.is_shutdown = true;
    }
}
