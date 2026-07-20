pub mod client;
pub mod protocol;
pub mod server;

use std::path::PathBuf;

pub fn socket_path() -> PathBuf {
    runtime_dir().join("btwattch2.sock")
}

pub fn pid_path() -> PathBuf {
    runtime_dir().join("btwattch2.pid")
}

fn runtime_dir() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

pub async fn is_daemon_available() -> bool {
    socket_path().exists() && client::ping().await.is_ok()
}
