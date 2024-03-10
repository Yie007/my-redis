use crate::{Connection, Frame};

/// 表示一个未知的命令。
///
/// 它的字段存储的是这个未知的命令，用于报错。
#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// 创建一个`Unknown`命令。
    pub(crate) fn new(value: impl ToString) -> Unknown {
        Unknown {
            command_name: value.to_string(),
        }
    }

    /// 响应客户端，表示此命令是未知的命令。
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("未知的命令：'{}'", self.command_name));

        dst.write_frame(&response).await?;
        Ok(())
    }

    /// 获取 command_name 字段。
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }
}
