pub mod client;
pub mod protocol;
pub mod server;

use std::path::{Path, PathBuf};

/// Where the agent daemon keeps its IPC socket and pid file.
pub struct AgentPaths {
    pub socket: PathBuf,
    pub pid: PathBuf,
}

impl AgentPaths {
    /// Derive the pid file from the socket path by swapping the extension
    /// (e.g. `btwattch2.sock` -> `btwattch2.pid`).
    fn pid_for(socket: &Path) -> PathBuf {
        socket.with_extension("pid")
    }
}

/// Default socket/pid locations under `$XDG_RUNTIME_DIR` (falling back to
/// `/tmp`), e.g. `$XDG_RUNTIME_DIR/btwattch2.sock`.
pub fn default_paths() -> AgentPaths {
    paths_from_socket(runtime_dir().join("btwattch2.sock"))
}

/// Build paths from an explicit socket location, deriving the pid file by
/// extension so the two stay co-located.
pub fn paths_from_socket(socket: PathBuf) -> AgentPaths {
    let pid = AgentPaths::pid_for(&socket);
    AgentPaths { socket, pid }
}

fn runtime_dir() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

pub async fn is_daemon_available(paths: &AgentPaths) -> bool {
    paths.socket.exists() && client::ping(paths).await.is_ok()
}
