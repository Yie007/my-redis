//! 提供表示 Redis 协议帧的类型，提供用于解析字节数组中的帧的实用工具。

use std::{fmt, io::Cursor, num::TryFromIntError, string::FromUtf8Error};

use bytes::{Buf, Bytes};

/// Redis 协议帧
/// 官方文档：https://redis.io/docs/reference/protocol-spec/
#[derive(Clone, Debug)]
pub enum Frame {
    // 简单字符串，通常用于表示响应，比如返回“OK”表示成功。
    // 例子：+OK\r\n
    Simple(String),

    // 错误，通常用于返回错误信息。
    // 格式：-<message>\r\n
    Error(String),

    // 整数，64位无符号十进制数，
    // 通常用于表示字节数或数组元素个数。
    // 格式：:<value>\r\n
    Integer(u64),

    // 大型字符串，通常用于表示字符串数据，长度任意。
    // 格式：$<length>\r\n<data>\r\n
    // bytes 库提供了零拷贝操作以及高效的切片操作，
    // 相比于直接操作大型字符串来说可以减少很多开销，因此使用 bytes::Bytes。
    Bulk(Bytes),

    // 数组类型。
    // 客户端发送的命令都是`Array`类型的`Frame`，
    // 一些返回元素集合的命令也是返回这个类型的`Frame`。
    // 格式：*<number-of-elements>\r\n<element-1>...<element-n>
    // 例子：*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
    Array(Vec<Frame>),

    // 空值，表示不存在的值。
    // 格式：_\r\n
    Null,
}

#[derive(Debug)]
pub enum Error {
    // 没有足够的数据来解析。
    Incomplete,

    // 其他错误。
    Other(crate::Error),
}

impl Frame {
    /// 返回一个空的帧数组，每个合法的命令都是一个帧数组。
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// 往帧数组中加入`Bulk`帧。
    ///
    /// # Panics
    ///
    /// 如果`self`不是一个数组，程序崩溃。
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("这不是一个帧数组"),
        }
    }

    /// 往帧数组中加入`Integer`帧。
    ///
    /// # Panics
    ///
    /// 如果`self`不是一个数组，程序崩溃。
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("这不是一个帧数组"),
        }
    }

    /// 检查是否可以从`src`中解码完整的信息。
    /// 此函数会移动`src`至数据末尾，即`\r\n`后。
    ///
    /// # Errors
    /// 如果`src`中数据不完整，即非`\r\n`结尾，返回`Err`。
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                // 尝试获取`Bulk`的字节个数。
                let len = TryInto::<usize>::try_into(get_decimal(src)?)?;
                // 保证光标处于末尾。
                // +2 表示略过`\r\n`。
                skip(src, len + 2)
            }
            b'*' => {
                // 获取`Array`的元素个数。
                let len = get_decimal(src)?;
                // 每个元素都必须是一个完整的`Frame`。
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            b'_' => {
                let _ = get_line(src)?;
                Ok(())
            }
            invalid => Err(format!("不合法的帧类型符：{}", invalid).into()),
        }
    }

    /// 解析数据为`Frame`，需要保证数据已经通过了`check()`。
    ///
    /// # Errors
    /// 如果数据无法解析为`Frame`，返回`Err`。
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // 获取行，转化为字节 vec。
                let line = get_line(src)?.to_vec();
                // 转化为字符串。
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                // 获取`Bulk`的字节个数。
                let len = TryInto::<usize>::try_into(get_decimal(src)?)?;
                // `src`中可用的字节数小于应该拥有的字节数。
                if src.remaining() < len + 2 {
                    return Err(Error::Incomplete);
                }
                let data = Bytes::copy_from_slice(&src.chunk()[0..len]);
                // 移动光标。
                skip(src, len + 2)?;
                Ok(Frame::Bulk(data))
            }
            b'*' => {
                // 获取Array的元素个数。
                let len = TryInto::<usize>::try_into(get_decimal(src)?)?;
                let mut result = Vec::with_capacity(len);
                // 解析每一个元素。
                for _ in 0..len {
                    result.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(result))
            }
            b'_' => {
                let line = get_line(src)?;
                if 0 == line.len() {
                    return Ok(Frame::Null);
                }
                Err("不合法的帧格式".into())
            }
            // 永远不会到达这里，但是为了通过编译。
            _ => unreachable!(),
        }
    }

    /// 将`Frame`转换为错误。
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("预料之外的Frame：{}", self).into()
    }
}

/// 获取`src`的第一个字节，这会移动光标。
///
/// # Errors
/// 如果`src`中剩余的字节数为 0，返回`Error::Incomplete`。
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    // 这是由Buf trait提供的函数，而`Cursor`实现了它。
    // 这个函数会自动移动光标。
    Ok(src.get_u8())
}

/// 将`src`往后移动 n 个字节。
///
/// # Errors
/// 如果`src`中剩余字节数小于 n，返回`Error::Incomplete`。
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    // `Cursor`自己的`advance()`不会删除数据，只是移动光标。
    src.advance(n);
    Ok(())
}

/// 获取一行，这里的行的末尾紧接`\r\n`。
/// 我们获取行的目的只是用于解析，返回引用可以减少由拷贝带来的损耗。
///
/// 这个函数会移动光标。
///
/// # Output
/// 假设`src`对应的数据为：`some-data\r\n`，那么这个函数会返回`some-data`。
/// 如果为：`data1\r\ndata2\r\n`，则返回`data1`，剩余数据为：`data2\r\n`。
/// 如果找不到`\r\n`，返回`Error::Incomplete`。
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    // `get_ref()`获取 &&[u8]。
    let end = src.get_ref().len() - 1;

    // 注意循环范围。
    for i in start..end {
        // 从前往后找，找到就直接返回。
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // 找到了一行，更新光标位置。
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

/// 获取一行，然后解析为十进制数。
///
/// 这个函数用于在解析`Array`的时候，获取`number-of-elements`，
/// 或者在解析`Bulk(Bytes)`的时候获取`length`。
///
/// # Errors
/// 如果数据不完整，或者数据为非 UTF-8 字符，或者无法解析为`u64`，则返回`Err`。
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let line = get_line(src)?;
    let u64_string =
        std::str::from_utf8(line).map_err(|_| Into::<Error>::into("不合法的帧格式"))?;
    let decimal = u64_string
        .parse::<u64>()
        .map_err(|_| Into::<Error>::into("不合法的帧格式"))?;
    Ok(decimal)
}

// 为了能将`frame::Error`转化为`Box<dyn std::error::Error + Send + Sync>`，必须实现。
impl std::error::Error for Error {}

// 为了`impl std::error::Error for Error`，必须实现。
impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => write!(fmt, "数据流过早关闭"),
            Error::Other(err) => write!(fmt, "{err}"),
        }
    }
}

// 为了能使用`into()`将`String`转换为`frame::Error`。
impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value.into())
    }
}

// 为了能使用`into()`将`&str`转换为`frame::Error`。
impl From<&str> for Error {
    fn from(value: &str) -> Self {
        // 委派给上面那个函数
        value.to_string().into()
    }
}

// `String::from_utf8()`可能抛出`FromUtf8Error`。
impl From<FromUtf8Error> for Error {
    fn from(_value: FromUtf8Error) -> Self {
        "不合法的帧格式".into()
    }
}

// `u64`转化为`usize`的`try_from()`可能抛出`TryFromIntError`。
impl From<TryFromIntError> for Error {
    fn from(_value: TryFromIntError) -> Self {
        "不合法的帧格式".into()
    }
}

// 方便进行比较。
impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // 使用空格作为分隔符。
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}
