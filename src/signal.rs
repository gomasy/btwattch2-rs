/// How this process should react to writing into a closed pipe.
///
/// Rust ignores SIGPIPE at startup, which suits a server but not a CLI: a
/// closed stdout turns into an EPIPE panic inside `println!`, and under
/// `panic = "abort"` that is a SIGABRT and a backtrace where every other CLI
/// exits quietly. Chosen once from the parsed command, since which behaviour is
/// wanted follows from what the process is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sigpipe {
    /// Die on a closed pipe, as `head` and friends expect. For the CLI.
    Fatal,
    /// Report EPIPE instead. For the agent, whose clients hang up routinely and
    /// which must survive it.
    Ignored,
}

/// Apply `mode` to this process. Call once, before any thread is spawned and
/// before anything is written to stdout.
pub fn set_sigpipe(mode: Sigpipe) {
    let handler = match mode {
        Sigpipe::Fatal => libc::SIG_DFL,
        Sigpipe::Ignored => libc::SIG_IGN,
    };
    // SAFETY: `signal` with a constant disposition is async-signal-safe, and
    // the sole caller runs before the runtime starts, so no other thread exists
    // to observe the change mid-flight.
    unsafe { libc::signal(libc::SIGPIPE, handler) };
}
