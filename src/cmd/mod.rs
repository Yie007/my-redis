mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod unknown;
pub use unknown::Unknown;

mod publish;
pub use publish::Publish;

mod subscribe;
pub use subscribe::Subscribe;

mod ping;
pub use ping::Ping;

use crate::{Connection, Db, Frame, Parse, Shutdown};

/// 支持的命令的枚举。
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Unknown(Unknown),
    Publish(Publish),
    Subscribe(Subscribe),
    Ping(Ping),
}

impl Command {
    /// 将`Frame`解析为`Command`
    /// 客户端发送的`Frame`是`Array`类型的
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // 将`Frame`转化为`Parse`，后者提供了类似迭代器的API
        // 方便我们进行解析
        // 如果`Frame`不是`Array`类型的，返回错误
        let mut parse = Parse::new(frame)?;
        // 客户端发送的`Array`类型的`Frame`的第一个元素
        // 是命令名称，他可以转换为字符串
        // 我们将其转换为全小写用于匹配
        let command_name = parse.next_string()?.to_lowercase();

        // 匹配命令名称，传递`Parse`用于解析为具体的命令
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "set" => Command::Set(Set::parse_frame(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                // 命令无法被识别
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // 检查是否还有剩余元素，如果有说明这`Frame`的格式有问题
        parse.finish()?;

        // 成功解析命令
        Ok(command)
    }

    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;
        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "publish",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Ping(_) => "ping",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
