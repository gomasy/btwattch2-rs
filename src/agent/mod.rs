pub mod client;
pub mod metrics;
pub mod protocol;
pub mod server;
pub mod status;

use std::path::{Path, PathBuf};

use btleplug::api::BDAddr;

/// Fallback runtime directory when `$XDG_RUNTIME_DIR` is unset. The agent needs
/// raw BLE access and so runs as root; `/run` is root-owned and lives on a
/// tmpfs that is cleared on boot, unlike the world-writable `/tmp` this used to
/// fall back to, where any local user could squat the socket or pre-create the
/// pid file as a symlink. The agent creates it 0700 on first start.
const RUNTIME_FALLBACK: &str = "/run/btwattch2";

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

    /// The pid the agent recorded, if the file holds one. A file that is
    /// missing, empty, or not a number all mean the same thing to every caller:
    /// there is no pid to go on.
    pub fn read_pid(&self) -> Option<u32> {
        std::fs::read_to_string(&self.pid).ok()?.trim().parse().ok()
    }

    /// Remove both files. Used when clearing a dead agent's leftovers and when
    /// a live one shuts down, so the pair is always torn down together.
    pub fn remove_files(&self) {
        std::fs::remove_file(&self.socket).ok();
        std::fs::remove_file(&self.pid).ok();
    }
}

/// Default socket/pid locations under `$XDG_RUNTIME_DIR` (falling back to
/// `/run/btwattch2`), e.g. `$XDG_RUNTIME_DIR/btwattch2.sock`.
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
        .unwrap_or_else(|| PathBuf::from(RUNTIME_FALLBACK))
}

/// What a live agent reported about itself when it answered a ping.
#[derive(Debug)]
pub struct DaemonInfo {
    /// The device the agent is attached to, or `None` from an agent too old to
    /// report one. Clients compare it against an explicit `--addr` so a command
    /// meant for one device is not silently served by an agent holding another.
    pub addr: Option<BDAddr>,
    /// The agent's own counters, or `None` from an agent too old to report them.
    pub status: Option<protocol::AgentStatus>,
}

/// Probe the agent socket, yielding `Some` only when an agent answers.
pub async fn probe_daemon(paths: &AgentPaths) -> Option<DaemonInfo> {
    if !paths.socket.exists() {
        return None;
    }
    client::ping(paths).await.ok()
}

/// Temp paths and cleanup shared by this module's test suites, so `client` and
/// `server` do not each invent a naming scheme and hand-rolled teardown.
#[cfg(test)]
pub mod testutil {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(0);

    /// A unique path under the temp dir, removed when the guard drops —
    /// including on a failing assert, which manual cleanup at the end of a test
    /// would skip. Names stay short because a unix socket path is capped at
    /// roughly 100 bytes.
    pub struct TempPath(PathBuf);

    impl TempPath {
        pub fn new(suffix: &str) -> Self {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!("bt2-{}-{id}{suffix}", std::process::id())))
        }

        pub fn path(&self) -> &Path {
            &self.0
        }

        /// A sibling path sharing this guard's lifetime, for tests that need a
        /// second file (a symlink target, say).
        pub fn sibling(&self, suffix: &str) -> PathBuf {
            let mut name = self.0.clone().into_os_string();
            name.push(suffix);
            PathBuf::from(name)
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            // The path may be a file, a symlink, or a directory tree.
            std::fs::remove_file(&self.0).ok();
            std::fs::remove_dir_all(&self.0).ok();
            for suffix in [".pid", ".victim"] {
                std::fs::remove_file(self.sibling(suffix)).ok();
            }
        }
    }
}
