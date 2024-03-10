pub mod client;

mod shutdown;
use shutdown::Shutdown;

mod connection;
pub use connection::Connection;

pub mod server;

pub mod frame;
pub use frame::Frame;

pub mod cmd;
pub use cmd::Command;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

/// 默认端口。
pub const DEFAULT_PORT: u16 = 6379;

/// 自定义的 Error，适配所有错误。
///
/// 当编写一个真实的应用的时候，我们可能会考虑一个专门用于处理错误的 crate
/// 或者将错误类型定义为原因的枚举。我们这里使用`std::error::Error`特征对象
/// 纯粹是为了方便，因为所有错误都能转换为它。
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// 自定义的 Result
///
/// 与我们自定义的 Error 一样，纯粹是为了方便。
pub type Result<T> = std::result::Result<T, Error>;
