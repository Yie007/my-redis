use crate::Db;
use crate::Frame;
use std::time::Duration;

use bytes::Bytes;

use crate::Connection;
use crate::Parse;
use crate::ParseError::EndOfStream;

/// 设置 key-value 对
///
/// 格式：Set <key> <value> [milliseconds]
///
/// 如果 key 已经有对应的 value 了，覆盖原有值，无论类型。
/// 在覆盖的同时也会清除原有键值对对应的“过期时间”。
#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    // 过期时间。
    expire: Option<Duration>,
}

impl Set {
    /// 创建一个`Set`命令。
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// 获取 key。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 获取 value。
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// 获取过期时间。
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// 负责应用命令并写回响应数据。
    ///
    /// 应用命令委派给了`Db`的方法。写回响应数据使用到了`Connection`，
    /// 如果写回响应错出错，返回`Err`。
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value, self.expire);
        // 写入响应信息
        let response = Frame::Simple("OK".to_string());
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// 通过`Parse`将`Frame`解析为`Set`命令。
    ///
    /// `Parse`提供了类似迭代器的 API 来解析`Frame`。
    /// 需要保证字符串`Set`已经被处理过了。
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::Result<Set> {
        // 字符串`Set`已经被处理过了，这里获取 key。
        let key = parse.next_string()?;
        // 获取 value。
        let value = parse.next_bytes()?;

        // 判断过期时间有没有设置。
        let mut expire = None;
        match parse.next_string() {
            // 过期时间，单位是毫秒。
            Ok(_) => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            // 如果没有设置，就为`None`。
            Err(EndOfStream) => {}
            // `next_string()`抛出的其他错误，这里直接抛出。
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    /// 将命令转换为等价的`Frame`
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(ms) = self.expire {
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }
}
