use astra_plugin_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_stream::StreamExt;

const STALZONE_API_BASE: &str = "https://eapi.stalzone.com";

#[astra::config]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Settings {
    /// STALZONE game region.
    #[serde(default = "default_region")]
    region: String,
    /// STALZONE application Client-Id. Never include this value in logs or responses.
    #[serde(default)]
    client_id: String,
    /// STALZONE application Client-Secret. Never include this value in logs or responses.
    #[serde(default)]
    client_secret: String,
    /// Polling interval in seconds.
    #[serde(default = "default_poll_interval")]
    poll_interval_secs: u64,
}

fn default_region() -> String {
    "RU".to_owned()
}

fn default_poll_interval() -> u64 {
    60
}

struct AstraEmissionMonitor {
    settings: Arc<Config<Settings>>,
    stopped: Arc<AtomicBool>,
    emission_state: Arc<Mutex<EmissionState>>,
    state_path: PathBuf,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct EmissionState {
    initialized: bool,
    region: Option<String>,
    current_start: Option<String>,
}

fn state_path() -> PathBuf {
    let config_dir = if cfg!(windows) {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("astra")
            .join("astra")
            .join("config")
    } else {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .unwrap_or_else(|| PathBuf::from("."))
            .join("astra")
    };

    config_dir
        .join("plugins")
        .join("stalzone-emission-monitor")
        .join("state.json")
}

fn load_state(path: &Path) -> EmissionState {
    fs::read_to_string(path)
        .ok()
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default()
}

fn save_state(path: &Path, state: &EmissionState) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("state path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("json.tmp");
    let json = serde_json::to_vec(state).map_err(std::io::Error::other)?;
    fs::write(&temporary, json)?;
    fs::rename(temporary, path)
}

impl Default for AstraEmissionMonitor {
    fn default() -> Self {
        Self {
            settings: Arc::new(Config::new(Settings {
                region: default_region(),
                poll_interval_secs: default_poll_interval(),
                ..Settings::default()
            })),
            stopped: Arc::new(AtomicBool::new(false)),
            state_path: state_path(),
            emission_state: Arc::new(Mutex::new(EmissionState::default())),
        }
    }
}

fn emission_started(
    state: &mut EmissionState,
    region: &str,
    current_start: Option<&str>,
) -> Option<String> {
    let region_changed = state.region.as_deref() != Some(region);
    let notification = if !state.initialized || region_changed {
        None
    } else if state.current_start.as_deref() != current_start {
        current_start.map(str::to_owned)
    } else {
        None
    };

    state.initialized = true;
    state.region = Some(region.to_owned());
    state.current_start = current_start.map(str::to_owned);
    notification
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct EmissionResponse {
    #[serde(rename = "currentStart", default)]
    current_start: Option<String>,
    #[serde(rename = "previousStart", default)]
    previous_start: Option<String>,
    #[serde(rename = "previousEnd", default)]
    previous_end: Option<String>,
}

struct EmissionApi {
    client: reqwest::Client,
}

impl EmissionApi {
    fn new() -> Result<Self, ToolError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("Astra Emission Monitor/0.1.0")
            .build()
            .map_err(ToolError::internal)?;
        Ok(Self { client })
    }

    async fn get_emission(&self, settings: &Settings) -> Result<EmissionResponse, ToolError> {
        if settings.client_id.trim().is_empty() {
            return Err(ToolError::not_configured_with(
                "client_id",
                "Configure the STALZONE Client ID in the plugin settings.",
            ));
        }
        if settings.client_secret.trim().is_empty() {
            return Err(ToolError::not_configured_with(
                "client_secret",
                "Configure the STALZONE Client Secret in the plugin settings.",
            ));
        }

        let region = match settings.region.as_str() {
            "RU" => "ru",
            "EU" => "eu",
            _ => {
                return Err(ToolError::BadArguments("Region must be RU or EU.".into()));
            }
        };

        let response = self
            .client
            .get(format!("{STALZONE_API_BASE}/{region}/emission"))
            .header("Client-Id", &settings.client_id)
            .header("Client-Secret", &settings.client_secret)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    ToolError::Timeout("STALZONE API request timed out.".into())
                } else {
                    ToolError::Unavailable("STALZONE API is unavailable.".into())
                }
            })?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(ToolError::Unauthorized(
                "STALZONE rejected the configured application credentials.".into(),
            ));
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ToolError::RateLimited {
                retry_after: None,
                message: "STALZONE rate limit reached; try again later.".into(),
            });
        }
        if !status.is_success() {
            return Err(ToolError::Unavailable(format!(
                "STALZONE API returned HTTP {}.",
                status.as_u16()
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|_| ToolError::Unavailable("Could not read the STALZONE response.".into()))?;
        serde_json::from_str(&body).map_err(|_| {
            ToolError::Internal("STALZONE returned an invalid emission response.".into())
        })
    }
}

#[astra::plugin]
impl AstraEmissionMonitor {
    async fn notify_emission(ctx: &PluginContext, start: &str) {
        let text = format!("STALZONE: началась новая эмиссия. Время начала: {start}.");
        match ctx.host().send_chat_message(&text, "", false).await {
            Ok(mut stream) => {
                while let Some(result) = stream.next().await {
                    if result.is_err() {
                        let _ = ctx
                            .host()
                            .log_warn("STALZONE notification was sent, but Astra returned an error while processing it.")
                            .await;
                        break;
                    }
                }
            }
            Err(_) => {
                let _ = ctx
                    .host()
                    .log_warn("Could not send the STALZONE emission notification to Astra chat.")
                    .await;
            }
        }
    }

    async fn poll_once(&self, ctx: &PluginContext) {
        let settings = self.settings.get();
        let api = match EmissionApi::new() {
            Ok(api) => api,
            Err(_) => return,
        };

        match api.get_emission(&settings).await {
            Ok(emission) => {
                let start = {
                    let mut state = self
                        .emission_state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let region = settings.region.as_str();
                    let notification =
                        emission_started(&mut state, region, emission.current_start.as_deref());
                    let _ = save_state(&self.state_path, &state);
                    notification
                };
                if let Some(start) = start {
                    Self::notify_emission(ctx, &start).await;
                }
            }
            Err(ToolError::NotConfigured { .. }) => {}
            Err(_) => {
                let _ = ctx
                    .host()
                    .log_warn("STALZONE emission check failed; monitoring will retry.")
                    .await;
            }
        }
    }

    /// Check whether a STALZONE emission is active in the configured region.
    #[tool]
    async fn current_emission(&self) -> Result<String, ToolError> {
        let settings = self.settings.load();
        let emission = EmissionApi::new()?.get_emission(&settings).await?;
        match emission.current_start {
            Some(start) => Ok(format!(
                "A STALZONE emission is active. It started at {start}."
            )),
            None => Ok("There is no STALZONE emission active right now.".into()),
        }
    }

    /// Report when the most recent recorded STALZONE emission started.
    #[tool]
    async fn last_emission(&self) -> Result<String, ToolError> {
        let settings = self.settings.load();
        let emission = EmissionApi::new()?.get_emission(&settings).await?;
        match emission.current_start.or(emission.previous_start) {
            Some(start) => Ok(format!("The last recorded STALZONE emission started at {start}.")),
            None => Ok(
                "STALZONE did not provide a previous emission start time in its response; the exact time is unavailable."
                    .into(),
            ),
        }
    }

    #[hook]
    async fn on_config(&self, _ctx: &PluginContext, settings: Settings) {
        self.settings.store(settings);
    }

    fn clone_for_monitoring(&self) -> Self {
        Self {
            settings: Arc::clone(&self.settings),
            stopped: Arc::clone(&self.stopped),
            emission_state: Arc::clone(&self.emission_state),
            state_path: self.state_path.clone(),
        }
    }

    #[hook]
    fn is_client(&self) -> bool {
        true
    }

    #[hook]
    async fn on_start(&self, ctx: &PluginContext) -> anyhow::Result<()> {
        self.stopped.store(false, Ordering::Release);
        let persisted = load_state(&self.state_path);
        self.emission_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone_from(&persisted);
        let plugin = self.clone_for_monitoring();
        let context = ctx.clone();
        tokio::spawn(async move {
            plugin.poll_once(&context).await;
            loop {
                if plugin.stopped.load(Ordering::Acquire) {
                    break;
                }
                let seconds = plugin.settings.load().poll_interval_secs.clamp(15, 3600);
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                if !plugin.stopped.load(Ordering::Acquire) {
                    plugin.poll_once(&context).await;
                }
            }
        });
        Ok(())
    }

    #[hook]
    async fn on_shutdown(&self, _ctx: &PluginContext) {
        self.stopped.store(true, Ordering::Release);
    }
}

astra::main!(AstraEmissionMonitor::default());

#[cfg(test)]
mod tests {
    use super::*;
    use astra_plugin_sdk::testing::Harness;

    /// `cargo test`. The harness runs the hooks in process against a recording
    /// host: no daemon, no socket, no Astra installed.
    ///
    /// `h.host().fired_triggers()` / `.logs()` / `.variables()` say what the
    /// plugin told Astra; `h.host().deny("fire_trigger")` stages the refusal a
    /// user's `[permissions]` would produce. For the wire — registration, the
    /// session token, streaming audio — there is
    /// `astra_plugin_sdk::testing::WireHarness`.
    #[tokio::test]
    async fn it_starts_and_answers() {
        let h = Harness::new(AstraEmissionMonitor::default())
            .with_config(json!({
                "region": "EU"
            }))
            .start()
            .await
            .expect("the plugin started");

        assert_eq!(h.tools().await.len(), 2);

        assert!(h.health().await.0);
    }

    #[tokio::test]
    async fn empty_config_uses_ru_and_does_not_expose_a_secret() {
        let _h = Harness::new(AstraEmissionMonitor::default())
            .with_config(json!({}))
            .start()
            .await
            .expect("the plugin started");

        let settings: Settings = serde_json::from_str("{}").expect("empty config parses");
        assert_eq!(settings.region, "RU");
        assert!(settings.client_id.is_empty());
        assert!(settings.client_secret.is_empty());
    }

    #[test]
    fn parses_only_documented_emission_fields() {
        let response: EmissionResponse = serde_json::from_str(
            r#"{
                "currentStart": "2026-10-25T12:00:00Z",
                "previousStart": "2026-10-24T12:00:00Z",
                "previousEnd": "2026-10-24T13:00:00Z"
            }"#,
        )
        .expect("documented response parses");

        assert_eq!(
            response.current_start.as_deref(),
            Some("2026-10-25T12:00:00Z")
        );
        assert_eq!(
            response.previous_start.as_deref(),
            Some("2026-10-24T12:00:00Z")
        );
        assert_eq!(
            response.previous_end.as_deref(),
            Some("2026-10-24T13:00:00Z")
        );
    }

    #[test]
    fn missing_previous_start_is_supported() {
        let response: EmissionResponse =
            serde_json::from_str(r#"{"currentStart": null}"#).expect("partial response parses");
        assert_eq!(
            response,
            EmissionResponse {
                current_start: None,
                previous_start: None,
                previous_end: None,
            }
        );
    }

    #[test]
    fn emission_notifications_are_deduplicated() {
        let mut state = EmissionState::default();

        assert_eq!(
            emission_started(&mut state, "RU", Some("2026-10-25T12:00:00Z")),
            None
        );
        assert_eq!(
            emission_started(&mut state, "RU", Some("2026-10-25T12:00:00Z")),
            None
        );
        assert_eq!(
            emission_started(&mut state, "RU", Some("2026-10-25T13:00:00Z")),
            Some("2026-10-25T13:00:00Z".into())
        );
        assert_eq!(emission_started(&mut state, "RU", None), None);
        assert_eq!(
            emission_started(&mut state, "RU", Some("2026-10-25T14:00:00Z")),
            Some("2026-10-25T14:00:00Z".into())
        );
    }

    #[test]
    fn emission_state_round_trips_without_credentials() {
        let path = std::env::temp_dir().join(format!(
            "stalzone-emission-monitor-state-{}.json",
            std::process::id()
        ));
        let expected = EmissionState {
            initialized: true,
            region: Some("RU".into()),
            current_start: Some("2026-10-25T12:00:00Z".into()),
        };

        save_state(&path, &expected).expect("state saves");
        assert_eq!(load_state(&path).current_start, expected.current_start);
        assert!(load_state(&path).initialized);
        let _ = std::fs::remove_file(path);
    }
}
