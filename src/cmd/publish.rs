use bytes::Bytes;

use crate::{parse::Parse, Connection, Db, Frame};

/// 向给定的广播信道发送信息。
///
/// 格式：Publish <channal> <message>
#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    /// 创建一个`Publish`命令。
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// 通过`Parse`将`Frame`解析为`Publish`命令。
    ///
    /// `Parse`提供了类似迭代器的 API 来解析`Frame`。
    /// 需要保证字符串`Publish`已经被处理过了。
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;
        Ok(Publish { channel, message })
    }

    /// 应用命令并写回响应数据。
    ///
    /// 应用命令委派给了`Db`的方法。写回响应数据使用到了`Connection`，
    /// 如果写回响应错出错，返回`Err`。
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // `State`中包含着`tokio::sync::broadcast::Sender`，
        // 应用`db.publish()`会将信息发送到对应广播信道。
        // 虽然返回值是订阅者的数量，但是这不代表实际接收到信息的订阅者，
        // 毕竟有可能在接收到信息前订阅者就 drop 掉了。
        let num_subscribe = db.publish(&self.channel, self.message);
        let response = Frame::Integer(num_subscribe as u64);
        // 写入响应数据。
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// 将命令转换为等价的`Frame`
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);
        frame
    }
}
