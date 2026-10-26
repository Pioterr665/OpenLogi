//! GUI navigation commands; these are not part of the agent's tarpc wire format.

use openlogi_core::brand::DeeplinkCommand;

/// A command delivered to the GUI's main event loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GuiCommand {
    /// An application-level command from a URL or tray item.
    App(DeeplinkCommand),
    /// Open the device identified by its opaque `DeviceStableId::runtime_key`.
    ShowDevice(String),
}
