//! Session-bus commands for the on-demand Linux GUI, independent of agent RPCs.

use crate::GuiCommand;
use openlogi_core::brand::DeeplinkCommand;
use openlogi_core::paths::Profile;
use tokio::sync::mpsc::UnboundedSender;

const PATH: &str = "/org/openlogi/Gui";
const INTERFACE: &str = "org.openlogi.Gui";

fn service_name() -> &'static str {
    match Profile::current() {
        Profile::Production => "org.openlogi.Gui",
        Profile::Dev => "org.openlogi.Gui.dev",
    }
}

struct GuiControl(UnboundedSender<GuiCommand>);

#[zbus::interface(name = "org.openlogi.Gui")]
impl GuiControl {
    fn show(&self) -> zbus::fdo::Result<()> {
        self.send(GuiCommand::App(DeeplinkCommand::Show))
    }

    fn quit(&self) -> zbus::fdo::Result<()> {
        self.send(GuiCommand::App(DeeplinkCommand::Quit))
    }

    fn show_device(&self, device_key: &str) -> zbus::fdo::Result<()> {
        if device_key.is_empty() {
            return Err(zbus::fdo::Error::InvalidArgs(
                "A device key is required".to_owned(),
            ));
        }
        self.send(GuiCommand::ShowDevice(device_key.to_owned()))
    }
}

impl GuiControl {
    fn send(&self, command: GuiCommand) -> zbus::fdo::Result<()> {
        self.0
            .send(command)
            .map_err(|_| zbus::fdo::Error::Failed("The GUI event loop has stopped".to_owned()))
    }
}

/// Publish the GUI's command endpoint. Retain the connection until GUI exit.
///
/// # Errors
/// Returns an error if the session bus or the profile's service name is unavailable.
pub async fn serve(commands: UnboundedSender<GuiCommand>) -> zbus::Result<zbus::Connection> {
    zbus::connection::Builder::session()?
        .name(service_name())?
        .serve_at(PATH, GuiControl(commands))?
        .build()
        .await
}

/// Focus an already-running GUI. `false` means no GUI owns the endpoint.
///
/// # Errors
/// Returns an error if the bus or the running GUI cannot accept the command.
pub async fn show() -> zbus::Result<bool> {
    send_show(None).await
}

/// Open a device in the running GUI. `false` means the GUI must be launched first.
///
/// # Errors
/// Returns an error if the bus or the running GUI cannot accept the command.
pub async fn show_device(device_key: &str) -> zbus::Result<bool> {
    send_show(Some(device_key)).await
}

async fn send_show(device_key: Option<&str>) -> zbus::Result<bool> {
    let connection = zbus::Connection::session().await?;
    if !is_running(&connection).await? {
        return Ok(false);
    }
    let proxy = zbus::Proxy::new(&connection, service_name(), PATH, INTERFACE).await?;
    if let Some(device_key) = device_key {
        proxy.call::<_, _, ()>("ShowDevice", &(device_key,)).await?;
    } else {
        proxy.call::<_, _, ()>("Show", &()).await?;
    }
    Ok(true)
}

async fn is_running(connection: &zbus::Connection) -> zbus::Result<bool> {
    Ok(zbus::fdo::DBusProxy::new(connection)
        .await?
        .name_has_owner(service_name().try_into()?)
        .await?)
}

/// Quit a running GUI and await its exit before the agent tears down.
///
/// Waiting for the bus name to disappear prevents its reconnect loop from
/// resurrecting the agent. Failure leaves the agent running so Quit can retry.
///
/// # Errors
/// Returns an error if delivery fails or the GUI does not exit within five seconds.
pub async fn quit() -> zbus::Result<()> {
    let connection = zbus::Connection::session().await?;
    if !is_running(&connection).await? {
        return Ok(());
    }
    let proxy = zbus::Proxy::new(&connection, service_name(), PATH, INTERFACE).await?;
    // The GUI may exit before its bus worker sends the method reply.
    if let Err(error) = proxy.call::<_, _, ()>("Quit", &()).await
        && is_running(&connection).await?
    {
        return Err(error);
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while is_running(&connection).await? {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(())
    })
    .await
    .map_err(|_| zbus::Error::Failure("The GUI did not exit after Quit".to_owned()))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_reach_the_gui_dispatcher_and_closed_dispatchers_reject_them() {
        let (commands, mut received) = tokio::sync::mpsc::unbounded_channel();
        let control = GuiControl(commands);
        control.show().unwrap();
        control.quit().unwrap();
        assert!(matches!(
            received.try_recv().unwrap(),
            GuiCommand::App(DeeplinkCommand::Show)
        ));
        assert!(matches!(
            received.try_recv().unwrap(),
            GuiCommand::App(DeeplinkCommand::Quit)
        ));
        drop(received);
        assert!(matches!(control.show(), Err(zbus::fdo::Error::Failed(_))));
    }

    #[test]
    fn device_commands_preserve_the_clicked_identity_and_reject_empty_keys() {
        let (commands, mut received) = tokio::sync::mpsc::unbounded_channel();
        let control = GuiControl(commands);
        for key in ["first-device", "second-device"] {
            control.show_device(key).unwrap();
            assert_eq!(
                received.try_recv().unwrap(),
                GuiCommand::ShowDevice(key.to_owned())
            );
        }
        assert!(matches!(
            control.show_device(""),
            Err(zbus::fdo::Error::InvalidArgs(_))
        ));
        assert!(matches!(
            received.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}
