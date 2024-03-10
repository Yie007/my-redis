//! my-redis-server
//!
//! 这个文件是服务器实现的入口点，使用了 clap 第三方库
//! 进行命令行参数解析

use clap::Parser;
use my_redis::server;
use my_redis::DEFAULT_PORT;
use tokio::net::TcpListener;
use tokio::signal;

#[derive(Parser, Debug)]
#[command(
    name = "my-redis-server",
    author,
    version,
    about = "一个自实现的Redis服务器"
)]
struct Args {
    // 解析参数，获取服务器端口。
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[test]
fn verify_args() {
    // clap 库提供的测试，可以帮助找出绝大部分的开发错误。
    use clap::CommandFactory;
    Args::command().debug_assert();
}

#[tokio::main]
pub async fn main() {
    // 获取命令行参数。
    let args = Args::parse();
    // 监听。
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();
    // 运行。
    server::run(listener, signal::ctrl_c()).await;
}
