use std::{
    io::{Error, ErrorKind},
    time::Duration,
};

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    cmd::{Get, Ping, Publish, Set, Subscribe},
    Connection, Frame,
};

/// 负责与Redis服务器建立连接。
pub struct Client {
    connection: Connection,
}

/// 一个进入了发布/订阅模式的客户端。
pub struct Subscriber {
    client: Client,
    subscribed_channels: Vec<String>,
}

/// 从订阅信道中获取到的信息。
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// 与服务器建立连接，创建`Client`。
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }

    /// 获取 key 对应的 value。对应`Get`命令。
    ///
    /// # Output
    /// 如果成功获取，返回`Ok(Some(data))`；
    /// 如果没有对应的 key，返回`Ok(None)`；
    /// 如果发送请求或读取响应出错，返回`Err`。
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // 创建一个`Get`命令并转化为`Frame`。
        let frame = Get::new(key).into_frame();
        // 写入`Get`请求。
        self.connection.write_frame(&frame).await?;

        // 等待响应帧。
        // 处理`Simple`和`Bulk`，`Null`表示 key 不存在。
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// 设置 key-entry，未设置过期时间。对应`Set`命令。
    ///
    /// # Errors
    /// 如果发送请求或读取响应出错，返回`Err`。
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        // 创建一个`Set`然后传递给`set_cmd()`，后者负责完成操作。
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// 设置 key-entry，设置了过期时间。对应`Set`命令。
    ///
    /// # Errors
    /// 如果发送请求或读取响应出错，返回`Err`。
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        // 创建一个`Set`然后传递给`set_cmd()`，后者负责完成操作。
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    /// 真正完成`Set`操作的核心函数。
    ///
    /// # Errors
    /// 如果发送请求或读取响应出错，返回`Err`。
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // 创建一个`Set`命令并转化为`Frame`。
        let frame = cmd.into_frame();
        // 写入`Get`请求。
        self.connection.write_frame(&frame).await?;

        // 等待响应帧。
        // 只处理`Simple`。
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// 向给定的信道发布信息。对应`Publish`命令。
    ///
    /// # Output
    /// 如果成功则返回订阅者的数量。如果发送请求或读取响应出错，返回`Err`。
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // 创建`Publish`并转换为`Frame`
        let frame = Publish::new(channel, message).into_frame();

        // 写入请求
        self.connection.write_frame(&frame).await?;

        // 等待响应
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// 订阅指定信道，将`Client`封装为`Subscriber`。对应`Subscribe`命令。
    ///
    /// # Output
    /// 如果成功则返回`Subscriber`。如果发送请求或读取响应出错，返回`Err`。
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        // 向客户端发出请求并等待响应
        self.subscribe_cmd(&channels).await?;

        // 转换为`Subscriber`
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    /// 真正完成`Subscribe`操作的核心函数
    ///
    /// # Errors
    /// 如果发送请求或读取响应出错，返回`Err`。
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // 转换命令为`Frame`
        let frame = Subscribe::new(channels.to_vec()).into_frame();

        // 写入请求
        self.connection.write_frame(&frame).await?;

        // 对于每个信道的订阅请求，服务端都会发送一个确认信息
        for channel in channels {
            // 读取响应
            let response = self.read_response().await?;

            // 验证，要求所有订阅请求都成功
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // 响应信息格式如下
                    // [ "subscribe", channel, num-subscribed ]
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }

    /// 测试连接。对应`Ping`命令。
    ///
    /// # Output
    /// 如果成功就返回响应数据。如果发送请求或读取响应出错，返回`Err`。
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// 从 socket 中读取响应帧。
    ///
    /// # Output
    /// 如果成功则返回读取到的响应帧。
    /// 如果读取响应帧失败，或者读取到`Frame::Error`，返回`Err`。
    /// 如果服务器关闭了，也返回`Err`。
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;
        match response {
            // 如果返回`Error Frame`，抛出错误
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                let err = Error::new(ErrorKind::ConnectionReset, "服务器关闭了连接");
                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// 获取已订阅的信道的信息，如果没有就等待。
    ///
    /// # Output
    /// 如果成功则返回`Ok(Some(msg))`。
    /// 返回`Ok(None)`表示`socket`关闭了。
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(mframe) => match mframe {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [message, channel, content] if *message == "message" => Ok(Some(Message {
                        channel: channel.to_string(),
                        content: Bytes::from(content.to_string()),
                    })),
                    _ => Err(mframe.to_error()),
                },
                frame => Err(frame.to_error()),
            },
            None => Ok(None),
        }
    }

    /// 发送信号帧，告诉服务端客户端已经关闭了，
    /// 让客户端结束`Subscriber`的`apply()`。
    ///
    /// # Errors
    /// 如果请求发送失败，返回`Err`。
    pub async fn send_ctrlc_frame(&mut self) -> crate::Result<()> {
        let frame = Frame::Simple("shutdown".to_string());
        self.client.connection.write_frame(&frame).await?;
        Ok(())
    }
}
