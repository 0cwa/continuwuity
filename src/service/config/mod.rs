use std::{ops::Deref, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use conduwuit::{
	Result, Server,
	config::{Config, check},
	error,
};

use crate::registration_tokens::{ValidToken, ValidTokenSource};

pub struct Service {
	server: Arc<Server>,
}

const SIGNAL: &str = "SIGUSR1";

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self { server: args.server.clone() }))
	}

	async fn worker(self: Arc<Self>) -> Result {
		while self.server.running() {
			if self.server.signal.subscribe().recv().await == Ok(SIGNAL) {
				if let Err(e) = self.handle_reload() {
					error!("Failed to reload config: {e}");
				}
			}
		}

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Deref for Service {
	type Target = Arc<Config>;

	#[inline]
	fn deref(&self) -> &Self::Target { &self.server.config }
}

impl Service {
	/// Get the registration token set in the config file, if it exists.
	#[must_use]
	pub fn get_config_file_token(&self) -> Option<ValidToken> {
		self.registration_token
			.clone()
			.map(|token| ValidToken { token, source: ValidTokenSource::Config })
	}

	fn handle_reload(&self) -> Result {
		if self.server.config.config_reload_signal {
			#[cfg(all(feature = "systemd", target_os = "linux"))]
			sd_notify::notify(&[
				sd_notify::NotifyState::Reloading,
				sd_notify::NotifyState::monotonic_usec_now()
					.expect("Failed to read monotonic time"),
			])
			.expect("failed to notify systemd of reloading state");

			let config_paths = self.server.config.config_paths.clone().unwrap_or_default();
			let reload_result = self.reload(&config_paths);

			#[cfg(all(feature = "systemd", target_os = "linux"))]
			let reload_result = finish_reload(reload_result, || {
				sd_notify::notify(&[sd_notify::NotifyState::Ready])
					.expect("failed to notify systemd of ready state");
			});

			#[cfg(not(all(feature = "systemd", target_os = "linux")))]
			let reload_result = finish_reload(reload_result, || {});

			reload_result.map(|_| ())?;
		}

		Ok(())
	}

	pub fn reload(&self, paths: &[PathBuf]) -> Result<Arc<Config>> {
		let old = self.server.config.clone();
		let new = Config::load(paths).and_then(|raw| Config::new(&raw))?;

		check::reload(&old, &new)?;
		self.server.config.update(new)
	}
}

fn finish_reload<T>(reload_result: Result<T>, notify_ready: impl FnOnce()) -> Result<T> {
	notify_ready();
	reload_result
}

#[cfg(test)]
mod tests {
	use std::cell::Cell;

	use conduwuit::{Err, Result};

	use super::finish_reload;

	#[test]
	fn rejected_reload_still_notifies_ready_and_returns_the_original_error() {
		let ready_notified = Cell::new(false);
		let reload_result: Result<()> = Err!("original reload rejection");

		let error = finish_reload(reload_result, || ready_notified.set(true))
			.expect_err("rejected reload should remain an error");

		assert!(ready_notified.get());
		assert!(error.to_string().contains("original reload rejection"));
	}
}
