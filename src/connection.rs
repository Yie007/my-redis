use std::io::{self, Cursor};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::Frame;

/// 发送和接收`Frame`值。
///
/// 当实现网络协议的时候，一个协议信息通常是由多个更小的称为帧的信息组成的。
/// `Connection`的目的就是从底层`TcpStream`中读取`Frame`或向其写入`Frame`。
#[derive(Debug)]
pub struct Connection {
    // `Tcpstream`用`BufWriter`封装，目的是提供异步的缓存写。
    stream: BufWriter<TcpStream>,

    // 读取帧时用到的缓存。`BytesMut`实现了 BufMut trait，
    // 它会在需要的时候隐式地扩大空间。
    buffer: BytesMut,
}

impl Connection {
    /// 创建一个`Connection`，同时初始化缓存。
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 使用4KB的读缓存即可，反正它会按照需要自动增长。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从连接中读取一个完整的`Frame`。
    ///
    /// 此函数会一直工作直到能读取到完整的`Frame`。假如读取到的数据不足以
    /// 解析为`Frame`，这些数据会被存储在缓存中，等待下一次的循环。
    ///
    /// # Output
    /// 如果成功解析出`Frame`，返回`Ok(Some(frame))`；
    /// 如果 socket 正常关闭，没有数据了，返回`Ok(Some(None))`；
    /// 如果 socket 意外关闭，数据不完整，返回`Err`；
    /// 如果 socket 读写发生错误也会返回`Err`。
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从缓存中解析`Frame`，如果缓存中的数据完整，
            // 那么解析出的`Frame`将会被返回。
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 如果缓存中没有足够的数据，尝试从 socket 中读取更多数据。
            // 如果返回的值是`0`，表明 socket 中已经没有数据了。
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 若已经达到了数据流的末尾，说明对方关闭了 socket。
                // 如果缓存中没有数据，说明对方是正常关闭的。
                // 否则说明有数据帧是不完整的，对方在发送的时候意外关闭了。
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("连接意外关闭".into());
                }
            }
        }
    }

    /// 尝试从缓存中解析`Frame`。
    ///
    /// # Errors
    /// 如果发现是不合法的数据帧，返回`Err`；
    /// 如果缓存中数据不完整，返回`Ok(None)`；如果解析成功，返回`Ok(Some(frame))`。
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use crate::frame::Error::Incomplete;

        // `Cursor`顾名思义是一个“光标”，可以看作是缓存的指针，跟踪字节。
        // `Cursor`实现了`bytes`库中的`Buf`，它提供了很多操作字节的工具。
        // 我们将缓存用`Cursor`包装，方便使用。
        let mut buf = Cursor::new(&self.buffer[..]);

        // 第一步检查是否有足够的数据来解析为一个数据帧。
        // 这一步比真正的解析快很多，可以提高效率。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // 保留数据帧的字节长度。
                let len = buf.position() as usize;

                // `check()`会将光标移动到帧的末尾，所以我们要在`parse()`
                // 前将光标位置重置回去。
                buf.set_position(0);

                // 真正完成解析任务的函数。。
                // 如果解析成功，返回`Frame`，
                // 如果解码的数据帧是不合法的，抛出错误。
                let frame = Frame::parse(&mut buf)?;

                // 将已经处理过的数据从读缓存中移除。
                // 当`advance()`被调用时，前面`len`长度的数据将被丢弃。
                // 详细工作由`BytesMut`完成，可能是通过移动内置的光标，
                // 也可能是通过内存重新分配和数据拷贝。
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // 读缓存中没有足够的数据来解析，等待继续读取数据到缓存中。
            // 我们不希望返回`Err`，因为它只是一种预期之中的运行状态。
            Err(Incomplete) => Ok(None),
            // 解析`Frame`时出现错误，返回`Err`，这最终会使得这个连接开始关闭。
            Err(e) => Err(e.into()),
        }
    }

    /// 向底层`TcpStream`中写入`Frame`，这里是`Array Frame`。
    ///
    /// 我们使用`AsyncWrite`提供的写函数。之所以不使用`TcpStream`
    /// 提供的写函数，是因为每次调用都会产生一次系统调用。而使用缓存
    /// 可以让数据先写入缓存，然后等缓存满后再使用一次系统调用写入。
    /// 需要注意的是，所有的数据都应该是字节数组，非字节数组的数据需要我们转换。
    ///
    /// # Errors
    /// 异步写可能会出现 I/O 错误。
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            // 由于异步方法不支持递归调用，因此只能分开讨论。
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;
                for entry in val.iter() {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }
        // 上面的调用实际上只是写入到缓存中。
        // 下面的调用确保缓存中的数据都写入了 socket 中。
        self.stream.flush().await
    }

    /// 写入非`Array Frame`。
    ///
    /// # Errors
    /// 异步写可能会出现 I/O 错误。
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"_\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 异步函数不支持递归，我们分开讨论`Array Frame`与其他。
            // 我们的 Redis 也不需要支持嵌套的`Array Frame`，
            // 所以这里不会执行到。
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// 写入`u64`以及`\r\n`。
    ///
    /// # Errors
    /// 异步写可能会出现 I/O 错误。
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        // 转换为`String`然后写入到字节数组
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
