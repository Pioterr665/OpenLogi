//! Settings-app infrastructure services.

pub mod assets;
pub(crate) mod device_reads;
pub mod diagnostics;
pub(crate) mod disable_keys_reads;
#[cfg(target_os = "linux")]
pub(crate) mod gui_control;
pub mod i18n;
pub mod ipc;
