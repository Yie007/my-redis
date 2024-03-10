use std::pin::Pin;

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{db::Db, shutdown::Shutdown, Connection, Frame, Parse};

/// 订阅一个或多个广播信道。
///
/// 格式：Subscribe <channel> [<channel> ...]
///
/// 进入订阅者模式后，客户端无法进行除了退出以外的其他操作。
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// 异步信息流，信息的类型是`Bytes`。
///
/// 参考`StreamMap`的 example。
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// 创建一个`Subscribe`命令。
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// 通过`Parse`将`Frame`解析为`Subscribe`命令。
    ///
    /// `Parse`提供了类似迭代器的 API 来解析`Frame`。
    /// 需要保证字符串`Subscribe`已经被处理过了。
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use crate::ParseError::EndOfStream;
        // 至少有一个channel，如果没有，报错。
        let mut channels = vec![parse.next_string()?];
        // 循环获取剩余的channel。
        loop {
            match parse.next_string() {
                // 还有channel。
                Ok(s) => channels.push(s),
                // 没有channel了。
                Err(EndOfStream) => break,
                // 其他错误。
                Err(err) => return Err(err.into()),
            }
        }
        Ok(Subscribe { channels })
    }

    /// 应用命令并写回响应数据。
    ///
    /// 应用命令委派给了`Db`的方法。写回响应数据使用到了`Connection`，
    /// 如果写回响应错出错，返回`Err`。
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // 每个信道都使用一个`sync::broadcast`。
        // 信息会散布到所有对应的订阅者。
        // 由于客户端可以订阅多个信道，
        // 所以我们使用`StreamMap`合并所有异步信息流进行管理。
        let mut subscriptions = StreamMap::new();

        // 对所有订阅了的信道，都生成对应的异步流并加到`StreamMap`并且发送响应信息。
        // 处理过的信道从`channels`中移除。
        for channel_name in self.channels.drain(..) {
            subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
        }

        loop {
            // 等待下面三种情况其中之一发生。
            // - 从订阅了的信道中接收到了信息
            // - 接收到了客户端的关闭信号
            // - 接收到了服务端的关闭信号
            tokio::select! {
                // 接收信息。
                // 调用`next()`后，`StreamMap`会对他管理的所有异步流
                // 调用`next()`，尝试产生值。
                // 如果成功就返回异步流在`StreamMap`中对应的 key 以及产生的值。
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                // 客户端发来了关闭信号，停止接收信息并结束，达到安全状态。
                ctrlc_frame = dst.read_frame() => {
                    // 验证`Frame`
                    match ctrlc_frame {
                        // 取得了`Frame`
                        Ok(Some(ctrlc_frame)) => {
                            match ctrlc_frame{
                                // 预期的帧，结束
                                Frame::Simple(v) if v == "shutdown" => {
                                    return Ok(())
                                }
                                // 非预期的帧，忽略，继续循环
                                _ => {},
                            }
                        },
                        // `socket`关闭了当然也要结束
                        Ok(None) => return Ok(()),
                        // 出错也要结束
                        Err(err) => return Err(err)
                    }
                }
                // 如果接收到服务器的关闭信号，就应该停止接收信息并结束，以达到安全状态。
                _ = shutdown.recv() => {
                    return Ok(());
                }
            }
        }
    }

    /// 将命令转换为对应的`Frame`
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

/// 订阅信道，生成异步流并进行管理，同时写回响应信息。
async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    // 订阅信道。
    let mut rx = db.subscribe(channel_name.clone());

    let rx = Box::pin(async_stream::stream! {
        // 使用`stream!`生成异步流。
        // 异步流类似于迭代器，但是它只在需要的时候产生值而不是一次性产生所有值，
        // 特点有：异步产生值、惰性计算、非阻塞、顺序处理。
        loop {
            match rx.recv().await {
                // 将接收到的有效消息作为异步流的元素产生。
                Ok(msg) => yield msg,
                // 接收信息时有延迟，忽略，继续接收。
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // 将异步数据流放入`StreamMap`进行管理。
    subscriptions.insert(channel_name.clone(), rx);

    // 响应客户端。
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// 生成`Subscribe`命令的响应帧。
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 生成`Frame`，告知客户端哪个信道发送了什么信息。
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}
