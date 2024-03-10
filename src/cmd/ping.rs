use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;

/// 测试连接。
///
/// 格式：Ping [message]
///
/// 如果没有设置发送到信息，返回`PONG`，否则返回相同的信息。
#[derive(Debug)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    /// 创建一个`Ping`命令。
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// 通过`Parse`将`Frame`解析为`Ping`命令。
    ///
    /// `Parse`提供了类似迭代器的API来解析`Frame`。
    /// 需要保证字符串`Ping`已经被处理过了。
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::new(None)),
            Err(e) => Err(e.into()),
        }
    }

    /// 应用命令并写回响应数据。
    ///
    /// 应用命令委派给了`Db`的方法。写回响应数据使用到了`Connection`，
    /// 如果写回响应错出错，返回`Err`。
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };
        // 写回响应数据。
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// 将命令转化为等价的`Frame`。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }
}
