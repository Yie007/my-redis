use bytes::Bytes;

use crate::parse::Parse;
use crate::Connection;
use crate::Db;
use crate::Frame;

/// 根据 key 获取 value。
///
/// 格式：Get <key>
///
/// 如果 key 不能解析为 `String`则会出错，如果不存在这样的
/// key 则返回 `(nil)`。
#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    /// 创建一个`Get`命令。
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// 获取 key 值。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 应用命令并写回响应数据。
    ///
    /// 应用命令委派给了`Db`的方法。写回响应数据使用到了`Connection`，
    /// 如果写回响应错出错，返回`Err`。
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };
        // 写入响应信息。
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// 通过`Parse`将`Frame`解析为`Get`命令。
    ///
    /// `Parse`提供了类似迭代器的 API 来解析`Frame`。
    /// 需要保证字符串`Get`已经被处理过了。
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::Result<Get> {
        // 字符串`Get`已经被处理过了，剩下的值就是 key。
        // 如果下一个值不可以解析为`String`。
        // 或者没有下一个值就返回错误。
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    /// 将命令转化为等价的`Frame`。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
