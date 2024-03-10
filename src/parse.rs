use crate::Frame;
use bytes::{Buf, Bytes};
use std::{fmt, str, vec};

/// 用于解析命令的工具类。
///
/// 命令是由`Array`表示的。例如`Set Foo 1`这个命令，
/// 就是`*3\r\n$3\r\nSet\r\n$3\r\nFoo\r\n$1\r\n1\r\n`。
/// `Parse`使用一个`Array`生成，并且提供类似迭代器的 API。
#[derive(Debug)]
pub(crate) struct Parse {
    // Array Frame 的迭代器。
    parts: vec::IntoIter<Frame>,
}

/// 当解析`Frame`的时候可能出现的错误。
///
/// 只有`EndOfStream`这个错误是在运行时处理，
/// 其他错误都会导致连接关闭。
#[derive(Debug)]
pub(crate) enum ParseError {
    /// 由于数据帧已被消耗完，无法再获取值。
    EndOfStream,
    /// 其他错误。
    Other(crate::Error),
}

impl Parse {
    /// 创建一个`Parse`来解析`Frame`的内容。
    ///
    /// # Errors
    /// 如果`Frame`不是一个`Array`，返回错误。
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("期望是帧数组，但实际上为：{:?}", frame).into()),
        };
        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// 获取 Array Frame 里的下一个`Frame`。
    ///
    /// # Errors
    /// 如果没有`Frame`了，返回`ParseError::EndOfStream`。
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// 获取Array Frame里的下一个`Frame`并解析为`String`。
    ///
    /// # Errors
    /// 如果无法表示为`String`，返回`Err`。
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // 只处理`Simple`和`Bulk`。
            // 虽然`Error`也是用字符串表示的，但我们单独处理它。
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "非UTF-8编码的字符串".into()),
            frame => Err(format!("预期是Simple或Bulk类型，实际为：{:?}", frame).into()),
        }
    }

    /// 获取Array Frame里的下一个`Frame`并解析为`Bytes`。
    ///
    /// # Errors
    /// 如果无法表示为`Bytes`，返回`Err`。
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // 只处理`Simple`和`Bulk`。
            // `Error`我们单独处理它。
            Frame::Simple(s) => Ok(Bytes::from(s)),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("预期是Simple或Bulk类型，实际为：{:?}", frame).into()),
        }
    }

    /// 获取Array Frame里的下一个`Frame`并解析为`u64`。
    ///
    /// # Errors
    /// 如果无法表示为`u64`，返回`Err`。
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        match self.next()? {
            // 只处理`Simple`、`Bulk`、`Integer`。
            Frame::Integer(v) => Ok(v),
            Frame::Simple(s) => s
                .parse::<u64>()
                .map_err(|_| Into::<ParseError>::into("不合法的数字")),
            Frame::Bulk(data) => {
                let s = String::from_utf8(data.chunk().to_vec())
                    .map_err(|_| Into::<ParseError>::into("非UTF-8编码的字符串"))?;
                s.parse::<u64>()
                    .map_err(|_| Into::<ParseError>::into("不合法的数字"))
            }
            frame => Err(format!("预期是Simple、Bulk或Integer类型，实际为：{:?}", frame).into()),
        }
    }

    /// 确保`Array`中已经没有更多元素了。
    ///
    /// # Errors
    /// 如果还有未处理的元素，返回`Err`。
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("预期Array已经没有更多元素了，但实际相反".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(value: String) -> ParseError {
        ParseError::Other(value.into())
    }
}

impl From<&str> for ParseError {
    fn from(value: &str) -> ParseError {
        value.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => write!(f, "数据流过早结束"),
            ParseError::Other(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ParseError {}
