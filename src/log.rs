use std::sync::atomic::{AtomicBool, Ordering};

static DEBUG: AtomicBool = AtomicBool::new(false);

/// Enable informational output. Called once at startup from the CLI flag.
pub fn set_debug(enabled: bool) {
    DEBUG.store(enabled, Ordering::Relaxed);
}

pub fn enabled() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

/// Print an informational message to stderr, only when --debug is on.
/// Warnings and errors are printed unconditionally with plain `eprintln!`.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        if $crate::log::enabled() {
            eprintln!("[INFO] {}", format_args!($($arg)*));
        }
    };
}
