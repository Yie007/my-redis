use bytes::Bytes;
use clap::{Parser, Subcommand};
use my_redis::{client::Client, DEFAULT_PORT};
use std::{convert::Infallible, num::ParseIntError, str, time::Duration};
use tokio::signal;

#[derive(Parser, Debug)]
#[command(
    name = "my-redis-client",
    version,
    author,
    about = "一个自实现的Redis客户端"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
    #[arg(name = "hostname", long, default_value = "127.0.0.1")]
    // default_value 接受一个参数 default，类型为 &str
    host: String,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    // default_value_t 类似，参数类型为 &str，但是他会尝试转换为指定类型
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        // clap 从命令行自动获取的`&str`无法自动转换为`Bytes`，
        // 所以我们要提供一个解析器。
        #[arg(value_parser = bytes_from_str)]
        value: Bytes,
        // 同理提供一个解析器。
        // 如果这个字段没有设置，就为`None`,
        // 否则就调用解析器。
        #[arg(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    Publish {
        channel: String,
        #[arg(value_parser = bytes_from_str)]
        message: Bytes,
    },
    Subscribe {
        // clap 可以自动收集参数并构造成`Vec`。
        channels: Vec<String>,
    },
    Ping {
        #[arg(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

// `Infallible`表示永远不会错误
fn bytes_from_str(src: &str) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(src.to_string()))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> my_redis::Result<()> {
    // 获取命令行参数。
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    // 连接服务端。
    let mut client = Client::connect(&addr).await?;
    // 执行命令，解析响应。
    match args.command {
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                // key不存在
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK")
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Ping { msg } => {
            let msg = client.ping(msg).await?;
            println!("{:?}", msg);
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("必须指定至少一个广播信道".into());
            }
            let mut subscriber = client.subscribe(channels).await?;
            // 一旦客户端进入`Subscribe`后，他会一直循环等待信息，
            // 我们应该同时开启一个异步任务，监听客户端的关闭信号。
            // 当关闭信号来临时，停止接收信息，关闭客户端。
            //
            // 但与此同时，服务端的这个连接的`Handler`的`run()`并不会结束，
            // 因为`cmd.apply()`并没有结束。
            // 究其原因是`Subscriber`的`apply()`没有执行完成，`StreamMap`中的
            // 所有异步流都处于`tx.recv()`等待中。
            //
            // 所以客户端在关闭信号到来的时候，应该发送一个消息给服务端，
            // 告诉`Subscriber`的`apply()`你应该结束了。
            loop {
                tokio::select! {
                    _ = signal::ctrl_c() => {
                        // 客户端发送一个信号帧，告诉服务器客户端关闭了。
                        subscriber.send_ctrlc_frame().await?;
                        return Ok(())
                    }

                    res = subscriber.next_message() => {
                        match res {
                            Ok(Some(msg)) => {
                                println!("从信道“{}”中获取到信息：{:?}", msg.channel, msg.content);
                            },
                            // 服务端关闭了。
                            Ok(None) => {
                                println!("服务器已关闭");
                                return Ok(());
                            },
                            // 出错。
                            Err(err) => return Err(err),
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
