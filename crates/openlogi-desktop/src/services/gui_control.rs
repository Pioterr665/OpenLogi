//! Deliver Linux tray commands through the existing GUI command dispatcher.

use openlogi_ipc::GuiCommand;
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

pub(crate) fn spawn(commands: UnboundedSender<GuiCommand>) {
    if let Err(error) = openlogi_core::worker::spawn("openlogi-gui-control", move |runtime| {
        runtime.block_on(async move {
            match openlogi_ipc::gui_control::serve(commands).await {
                Ok(_connection) => std::future::pending::<()>().await,
                Err(error) => warn!(%error, "could not publish the GUI tray endpoint"),
            }
        });
    }) {
        warn!(%error, "could not start the GUI tray endpoint");
    }
}
