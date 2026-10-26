//! Linux `StatusNotifierItem` and `DBusMenu`, owned by the always-on agent.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ksni::TrayMethods as _;
use ksni::menu::StandardItem;
use openlogi_agent_core::observable::ObservableState;
use openlogi_core::config::{AppIcon, AppSettings};
use openlogi_core::device::{
    BatteryInfo, BatteryLevel, BatteryStatus, DeviceKind, prettify_codename,
};
use openlogi_core::device_order::DeviceStableId;
use openlogi_core::hid::{DIRECT_DEVICE_INDEX, DeviceRoute};
use openlogi_ipc::{AgentSnapshot, InventoryHealth, gui_control};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::shutdown::{self, ShutdownRequestSender};

static PREFERENCES: OnceLock<watch::Sender<Preferences>> = OnceLock::new();

#[derive(Clone, PartialEq, Eq)]
struct Preferences {
    enabled: bool,
    icon: AppIcon,
    language: Option<String>,
}

impl From<&AppSettings> for Preferences {
    fn from(settings: &AppSettings) -> Self {
        Self {
            enabled: settings.show_in_menu_bar,
            icon: settings.app_icon,
            language: settings.language.clone(),
        }
    }
}

/// Publish preferences after the process locale has been updated.
pub(crate) fn configure(settings: &AppSettings) {
    if let Some(preferences) = PREFERENCES.get() {
        preferences.send_replace(Preferences::from(settings));
    }
}

/// Start the tray without delaying hardware or IPC startup.
pub(crate) fn spawn(
    settings: &AppSettings,
    observable: Arc<ObservableState>,
    shutdown_tx: ShutdownRequestSender,
) -> tokio::task::JoinHandle<()> {
    let (preferences, receiver) = watch::channel(Preferences::from(settings));
    let _ = PREFERENCES.set(preferences);
    tokio::spawn(run(receiver, observable, shutdown_tx))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    Show,
    ShowDevice(String),
    Quit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TrayRow {
    label: String,
    action: Option<Action>,
}

struct Tray {
    rows: Vec<TrayRow>,
    icon: Vec<ksni::Icon>,
    icon_kind: AppIcon,
    device_icon_name: &'static str,
    actions: mpsc::UnboundedSender<Action>,
}

impl ksni::Tray for Tray {
    fn id(&self) -> String {
        "openlogi".to_owned()
    }

    fn title(&self) -> String {
        "OpenLogi".to_owned()
    }

    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        self.icon.clone()
    }

    fn icon_name(&self) -> String {
        self.device_icon_name.to_owned()
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        ksni::ToolTip {
            title: self.title(),
            icon_name: self.icon_name(),
            description: self
                .rows
                .iter()
                .map(|row| row.label.as_str())
                .collect::<Vec<_>>()
                .join("\n")
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;"),
            ..Default::default()
        }
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        let _ = self.actions.send(Action::Show);
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        let mut menu = vec![
            StandardItem {
                label: rust_i18n::t!("app.show_main_window").to_string(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.actions.send(Action::Show);
                }),
                ..Default::default()
            }
            .into(),
            ksni::MenuItem::Separator,
        ];
        menu.extend(self.rows.iter().map(|row| {
            let action = row.action.clone();
            StandardItem {
                label: row.label.clone(),
                enabled: action.is_some(),
                activate: Box::new(move |tray: &mut Self| {
                    if let Some(action) = action.clone() {
                        let _ = tray.actions.send(action);
                    }
                }),
                ..Default::default()
            }
            .into()
        }));
        menu.push(ksni::MenuItem::Separator);
        menu.push(
            StandardItem {
                label: rust_i18n::t!("app.quit_openlogi").to_string(),
                icon_name: "application-exit".to_owned(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.actions.send(Action::Quit);
                }),
                ..Default::default()
            }
            .into(),
        );
        menu
    }

    fn watcher_offline(&self, reason: ksni::OfflineReason) -> bool {
        warn!(
            ?reason,
            "tray host unavailable; GNOME needs an AppIndicator extension"
        );
        true
    }
}

async fn run(
    mut preferences: watch::Receiver<Preferences>,
    observable: Arc<ObservableState>,
    shutdown_tx: ShutdownRequestSender,
) {
    let mut observations = observable.subscribe();
    let (actions, mut requests) = mpsc::unbounded_channel();
    let mut handle: Option<ksni::Handle<Tray>> = None;
    let mut reconcile = tokio::time::interval(Duration::from_secs(5));
    loop {
        let settings = preferences.borrow_and_update().clone();
        let (rows, device_icon_name) = observable.read(|snapshot| {
            let kinds = snapshot
                .inventory
                .iter()
                .flat_map(|receiver| receiver.paired.iter().map(|device| device.kind))
                .chain(snapshot.standalone.iter().map(|device| device.kind));
            (device_rows(snapshot), single_device_icon(kinds))
        });
        if !settings.enabled {
            if let Some(tray) = handle.take() {
                tray.shutdown().await;
            }
        } else if let Some(tray) = &handle
            && !tray.is_closed()
        {
            let _ = tray
                .update(|tray| {
                    tray.rows = rows;
                    tray.device_icon_name = device_icon_name;
                    if tray.icon_kind != settings.icon {
                        tray.icon = glyph(settings.icon);
                        tray.icon_kind = settings.icon;
                    }
                })
                .await;
        } else {
            let tray = Tray {
                rows,
                icon: glyph(settings.icon),
                icon_kind: settings.icon,
                device_icon_name,
                actions: actions.clone(),
            };
            match tray.assume_sni_available(true).spawn().await {
                Ok(tray) => {
                    info!("Linux tray service started");
                    handle = Some(tray);
                }
                Err(error) => warn!(%error, "could not start Linux tray; retrying"),
            }
        }
        tokio::select! {
            result = observations.changed() => if result.is_err() { break; },
            result = preferences.changed() => if result.is_err() { break; },
            _ = reconcile.tick() => {},
            Some(action) = requests.recv() => match action {
                Action::Show => show_gui(None).await,
                Action::ShowDevice(device_key) => show_gui(Some(&device_key)).await,
                Action::Quit => {
                    match gui_control::quit().await {
                        Ok(()) => {
                            crate::overlay::evict_on_quit();
                            let _quit = tokio::task::spawn_blocking(move || {
                                shutdown::request_tray_quit(Some(&shutdown_tx), 0)
                            });
                            break;
                        }
                        Err(error) => warn!(%error, "could not close the GUI; leaving the agent running"),
                    }
                }
            },
        }
    }
    if let Some(tray) = handle {
        tray.shutdown().await;
    }
}

fn single_device_icon(mut devices: impl Iterator<Item = DeviceKind>) -> &'static str {
    let Some(kind) = devices.next() else {
        return "";
    };
    if devices.next().is_some() {
        return "";
    }
    // An empty theme name selects the embedded OpenLogi pixmap. Never choose
    // one device's icon to represent a list containing several devices.
    match kind {
        DeviceKind::Mouse | DeviceKind::Trackball => "input-mouse",
        DeviceKind::Keyboard | DeviceKind::Numpad => "input-keyboard",
        DeviceKind::Touchpad => "input-touchpad",
        DeviceKind::Camera => "camera-web",
        DeviceKind::Headset => "audio-headset",
        DeviceKind::Gamepad | DeviceKind::Joystick => "input-gaming",
        DeviceKind::Presenter
        | DeviceKind::Remote
        | DeviceKind::Tablet
        | DeviceKind::Unknown
        | DeviceKind::Light => "",
    }
}

fn device_rows(snapshot: &AgentSnapshot) -> Vec<TrayRow> {
    let mut rows = Vec::new();
    for receiver in &snapshot.inventory {
        for device in &receiver.paired {
            let name = device.codename.as_deref().map_or_else(
                || rust_i18n::t!("device.device").to_string(),
                prettify_codename,
            );
            let status = if !device.online {
                rust_i18n::t!("device.offline").to_string()
            } else {
                device.battery.as_ref().map_or_else(
                    || rust_i18n::t!("device.connected").to_string(),
                    battery_label,
                )
            };
            rows.push(TrayRow {
                label: format!("{name} — {status}"),
                action: Some(Action::ShowDevice(
                    DeviceStableId::from_parts(
                        DeviceRoute::for_slot(receiver, device.slot).as_ref(),
                        device.slot,
                        device
                            .model_info
                            .as_ref()
                            .and_then(|model| model.serial_number.as_deref()),
                        device
                            .model_info
                            .as_ref()
                            .map_or([0; 4], |model| model.unit_id),
                    )
                    .runtime_key(),
                )),
            });
        }
    }
    for device in &snapshot.standalone {
        let status = if device.online {
            "device.connected"
        } else {
            "device.offline"
        };
        rows.push(TrayRow {
            label: format!("{} — {}", device.display_name, rust_i18n::t!(status)),
            action: Some(Action::ShowDevice(
                DeviceStableId::from_parts(
                    Some(&device.route()),
                    DIRECT_DEVICE_INDEX,
                    device.serial_number.as_deref(),
                    device.unit_id,
                )
                .runtime_key(),
            )),
        });
    }
    if rows.is_empty() {
        let key = match snapshot.status.inventory {
            InventoryHealth::Ready => "device.no_devices_connected",
            InventoryHealth::Scanning => "agent.scanning_for_devices",
            InventoryHealth::Unavailable => "agent.device_scanning_is_unavailable",
        };
        rows.push(TrayRow {
            label: rust_i18n::t!(key).to_string(),
            action: None,
        });
    }
    rows
}

fn battery_label(battery: &BatteryInfo) -> String {
    // Charging is the primary status here; firmware may not have a charge
    // reading yet. The full battery details remain in the device window.
    match battery.status {
        BatteryStatus::Charging | BatteryStatus::ChargingSlow => {
            return rust_i18n::t!("device.charging").to_string();
        }
        BatteryStatus::Full => return rust_i18n::t!("device.full").to_string(),
        BatteryStatus::Error => return rust_i18n::t!("device.battery_error").to_string(),
        BatteryStatus::Discharging | BatteryStatus::Unknown => {}
    }
    if let Some(percentage) = battery.percentage {
        return format!("{percentage}%");
    }
    let key = match battery.level {
        BatteryLevel::Critical | BatteryLevel::Low => "device.low_battery",
        BatteryLevel::Good => "device.battery_good",
        BatteryLevel::Full => "device.full",
        BatteryLevel::Unknown => return format!("{} —", rust_i18n::t!("device.battery")),
    };
    rust_i18n::t!(key).to_string()
}

fn glyph(icon: AppIcon) -> Vec<ksni::Icon> {
    let png: &[u8] = match icon {
        AppIcon::Openlogi => include_bytes!("../assets/tray-icon@2x.png"),
        AppIcon::Prism => include_bytes!("../assets/tray-icon-prism@2x.png"),
    };
    let Ok(image) = image::load_from_memory(png) else {
        return Vec::new();
    };
    let image = image.to_rgba8();
    let (Ok(width), Ok(height)) = (i32::try_from(image.width()), i32::try_from(image.height()))
    else {
        return Vec::new();
    };
    // SNI uses network-order ARGB, unlike image's RGBA storage. Neutral gray
    // keeps this monochrome glyph visible on both light and dark panels.
    let data = image
        .pixels()
        .flat_map(|pixel| [pixel.0[3], 128, 128, 128])
        .collect();
    vec![ksni::Icon {
        width,
        height,
        data,
    }]
}

async fn show_gui(device_key: Option<&str>) {
    let result = match device_key {
        Some(device_key) => gui_control::show_device(device_key).await,
        None => gui_control::show().await,
    };
    match result {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            warn!(%error, "could not contact the GUI from the tray");
            return;
        }
    }
    let Some(gui) = std::env::current_exe()
        .ok()
        .and_then(|exe| {
            exe.parent()
                .map(|directory| directory.join("openlogi-desktop"))
        })
        .filter(|path| path.is_file())
    else {
        warn!("no GUI binary found next to the agent");
        return;
    };
    match std::process::Command::new(&gui).spawn() {
        Ok(mut child) => {
            let _gui_waiter = std::thread::spawn(move || {
                if let Err(error) = child.wait() {
                    warn!(%error, "could not reap the tray-launched GUI");
                }
            });
            if let Some(device_key) = device_key {
                // The service may start before inventory; the GUI retains the
                // request until it can resolve the device's detail view.
                let delivered = tokio::time::timeout(Duration::from_secs(10), async {
                    while !gui_control::show_device(device_key).await? {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok::<(), zbus::Error>(())
                })
                .await;
                if !matches!(delivered, Ok(Ok(()))) {
                    warn!(
                        ?delivered,
                        "could not deliver the device selection to the launched GUI"
                    );
                }
            }
        }
        Err(error) => {
            warn!(%error, path = %gui.display(), "could not launch the GUI from the tray")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panel_icon_tracks_single_device_and_returns_to_openlogi_for_multiple_devices() {
        assert_eq!(
            single_device_icon([DeviceKind::Mouse].into_iter()),
            "input-mouse"
        );
        assert_eq!(
            single_device_icon([DeviceKind::Keyboard].into_iter()),
            "input-keyboard"
        );
        assert_eq!(
            single_device_icon([DeviceKind::Mouse, DeviceKind::Keyboard].into_iter()),
            ""
        );
        assert_eq!(
            single_device_icon([DeviceKind::Mouse, DeviceKind::Mouse].into_iter()),
            ""
        );
        assert_eq!(single_device_icon([DeviceKind::Unknown].into_iter()), "");
        assert_eq!(single_device_icon(std::iter::empty()), "");
        assert_eq!(
            single_device_icon([DeviceKind::Mouse].into_iter()),
            "input-mouse"
        );
    }

    #[test]
    fn device_items_are_enabled_and_clicks_keep_their_own_target() {
        use ksni::Tray as _;
        let (actions, mut received) = mpsc::unbounded_channel();
        let tray = Tray {
            rows: ["first-device", "second-device"]
                .into_iter()
                .map(|key| TrayRow {
                    label: "Same model — 80%".to_owned(),
                    action: Some(Action::ShowDevice(key.to_owned())),
                })
                .collect(),
            icon: Vec::new(),
            icon_kind: AppIcon::Openlogi,
            device_icon_name: "",
            actions,
        };
        let menu = tray.menu();
        let ksni::MenuItem::Standard(item) = &menu[3] else {
            panic!("second device item");
        };
        assert!(item.enabled, "a device must not look unavailable");
        let mut tray = tray;
        (item.activate)(&mut tray);
        assert_eq!(
            received.try_recv().unwrap(),
            Action::ShowDevice("second-device".to_owned())
        );
    }

    #[test]
    fn charging_without_a_reading_is_not_displayed_as_an_empty_battery() {
        for status in [BatteryStatus::Charging, BatteryStatus::ChargingSlow] {
            let battery = BatteryInfo {
                percentage: Some(0),
                level: BatteryLevel::Unknown,
                status,
            };
            assert_eq!(
                battery_label(&battery),
                rust_i18n::t!("device.charging").to_string()
            );
        }
    }

    #[test]
    fn absent_percentage_is_not_invented_from_a_coarse_level() {
        let battery = BatteryInfo {
            percentage: None,
            level: BatteryLevel::Low,
            status: BatteryStatus::Discharging,
        };
        assert_eq!(
            battery_label(&battery),
            rust_i18n::t!("device.low_battery").to_string()
        );
        assert_eq!(
            battery_label(&BatteryInfo {
                percentage: Some(0),
                ..battery
            }),
            "0%"
        );
    }

    #[test]
    fn an_empty_inventory_distinguishes_scanning_failure_and_no_devices() {
        let state = ObservableState::new("test".to_owned());
        let mut snapshot = state.snapshot();
        for (health, key) in [
            (InventoryHealth::Scanning, "agent.scanning_for_devices"),
            (
                InventoryHealth::Unavailable,
                "agent.device_scanning_is_unavailable",
            ),
            (InventoryHealth::Ready, "device.no_devices_connected"),
        ] {
            snapshot.status.inventory = health;
            assert_eq!(
                device_rows(&snapshot),
                vec![TrayRow {
                    label: rust_i18n::t!(key).to_string(),
                    action: None
                }]
            );
        }
    }

    #[test]
    fn both_glyphs_have_valid_argb_payloads_and_visible_pixels() {
        for kind in [AppIcon::Openlogi, AppIcon::Prism] {
            let icons = glyph(kind);
            assert_eq!(icons.len(), 1);
            let icon = &icons[0];
            let pixels = usize::try_from(icon.width * icon.height).unwrap();
            assert_eq!(icon.data.len(), pixels * 4);
            assert!(icon.data.chunks_exact(4).any(|pixel| pixel[0] > 0));
            assert!(
                icon.data
                    .chunks_exact(4)
                    .all(|pixel| pixel[1..] == [128, 128, 128])
            );
        }
    }
}
