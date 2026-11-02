mod data;
pub(super) mod migrations;
pub mod mxc;
mod preview;
mod remote;
mod store;
mod tests;
mod thumbnail;
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use conduwuit::{
	Err, Result, Server, debug, debug_error, debug_info, debug_warn, err, error,
	metrics::KeyValue,
	trace,
	utils::{
		self, MutexMap, MutexMapGuard,
		time::{self, TimeDirection},
	},
	warn,
};
use ruma::{OwnedMxcUri, UserId, http_headers::ContentDisposition};

use self::data::{Data, Metadata};
use self::store::{MediaStore, legacy_path, sha256_path};
pub use self::thumbnail::Dim;
use crate::{Dep, client, globals, media::mxc::Mxc, moderation, sending};

#[derive(Debug)]
pub struct FileMeta {
	pub content: Option<Vec<u8>>,
	pub content_type: Option<String>,
	pub content_disposition: Option<ContentDisposition>,
}

pub struct Service {
	url_preview_mutex: MutexMap<String, ()>,
	mxc_mutex: MediaMutex,
	pub(super) db: Data,
	store: MediaStore,
	services: Services,
}

#[derive(Default)]
struct MediaMutex(MutexMap<String, ()>);

impl MediaMutex {
	async fn lock(&self, mxc: &Mxc<'_>) -> MutexMapGuard<String, ()> {
		let key = mxc.to_string();
		self.0.lock(&key).await
	}
}

struct Services {
	server: Arc<Server>,
	client: Dep<client::Service>,
	globals: Dep<globals::Service>,
	sending: Dep<sending::Service>,
	moderation: Dep<moderation::Service>,
}

/// generated MXC ID (`media-id`) length
pub const MXC_LENGTH: usize = 32;

/// Cache control for immutable objects.
pub const CACHE_CONTROL_IMMUTABLE: &str = "public,max-age=31536000,immutable";

/// Default cross-origin resource policy.
pub const CORP_CROSS_ORIGIN: &str = "cross-origin";

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let store = MediaStore::build(&args.server.config)?;
		Ok(Arc::new(Self {
			url_preview_mutex: MutexMap::new(),
			mxc_mutex: MediaMutex::default(),
			db: Data::new(args.db),
			store,
			services: Services {
				server: args.server.clone(),
				client: args.depend::<client::Service>("client"),
				globals: args.depend::<globals::Service>("globals"),
				sending: args.depend::<sending::Service>("sending"),
				moderation: args.depend::<moderation::Service>("moderation"),
			},
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> {
		self.create_media_dir().await?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Uploads a file.
	pub async fn create(
		&self,
		mxc: &Mxc<'_>,
		user: Option<&UserId>,
		content_disposition: Option<&ContentDisposition>,
		content_type: Option<&str>,
		file: &[u8],
	) -> Result<()> {
		let _mxc_lock = self.mxc_mutex.lock(mxc).await;

		// Width, Height = 0 if it's not a thumbnail
		let key = self
			.db
			.prepare_file_metadata(mxc, &Dim::default(), content_disposition, content_type)
			.map_err(|error| {
				err!(Database(error!("Failed to prepare media metadata for MXC {mxc}: {error}")))
			})?;

		self.store.put(&key, file).await.map_err(|error| {
			err!(Database(error!(
				"Failed to write media file for MXC {mxc} at key {key:?}: {error}"
			)))
		})?;
		self.db.insert_file_metadata(&key, mxc, user);

		self.services
			.server
			.metrics
			.instruments
			.media_write_size
			.record(file.len().try_into().unwrap_or(u64::MAX), &[self.origin(mxc)]);

		Ok(())
	}

	fn origin(&self, mxc: &Mxc<'_>) -> KeyValue {
		let origin = if self.services.globals.server_is_ours(mxc.server_name) {
			"local"
		} else {
			"remote"
		};

		KeyValue::new("origin", origin)
	}

	/// Deletes a file in the database and from the media directory via an MXC
	pub async fn delete(&self, mxc: &Mxc<'_>) -> Result<()> {
		let _mxc_lock = self.mxc_mutex.lock(mxc).await;
		self.delete_locked(mxc).await
	}

	/// Deletes media for an MXC while the caller holds its media lock.
	async fn delete_locked(&self, mxc: &Mxc<'_>) -> Result<()> {
		let keys = self.db.search_mxc_metadata_prefix(mxc).await;
		if keys.is_empty() {
			return Err!(Database(error!(
				"Failed to find any media keys for MXC {mxc} in our database"
			)));
		}

		self.delete_keys_locked(mxc, &keys).await
	}

	/// Deletes the supplied objects and their database metadata while the
	/// caller holds the MXC lock.
	async fn delete_keys_locked(&self, mxc: &Mxc<'_>, keys: &[Vec<u8>]) -> Result<()> {
		for key in keys {
			trace!(%mxc, "MXC Key: {key:?}");
			debug_info!(%mxc, "Deleting media object");
			if let Err(error) = self.store.delete(key).await {
				debug_error!(%mxc, ?key, "Failed to remove media object: {error}");
				if !self.store.is_local() {
					return Err(error);
				}
			}
		}

		debug_info!(%mxc, "Deleting media metadata from database");
		self.db.delete_file_mxc(mxc).await;
		Ok(())
	}

	/// Deletes all media by the specified user
	///
	/// currently, this is only practical for local users
	pub async fn delete_from_user(&self, user: &UserId) -> Result<usize> {
		let mxcs = self.db.get_all_user_mxcs(user).await;
		let mut deletion_count: usize = 0;

		for mxc in mxcs {
			let Ok(mxc) = mxc.as_str().try_into().inspect_err(|e| {
				debug_error!(%mxc, "Failed to parse MXC URI from database: {e}");
			}) else {
				continue;
			};

			debug_info!(%deletion_count, "Deleting MXC {mxc} by user {user} from database and filesystem");
			match self.delete(&mxc).await {
				| Ok(()) => {
					deletion_count = deletion_count.saturating_add(1);
				},
				| Err(e) => {
					debug_error!(%deletion_count, "Failed to delete {mxc} from user {user}: {e}");
					propagate_remote_media_storage_error(self.store.is_local(), e)?;
				},
			}
		}

		Ok(deletion_count)
	}

	/// Downloads a file.
	pub async fn get(&self, mxc: &Mxc<'_>) -> Result<Option<FileMeta>> {
		match self.db.search_file_metadata(mxc, &Dim::default()).await {
			| Ok(Metadata { content_disposition, content_type, key }) => {
				let content = self.store.get(&key).await?;

				self.services
					.server
					.metrics
					.instruments
					.media_read_size
					.record(content.len().try_into().unwrap_or(u64::MAX), &[self.origin(mxc)]);

				Ok(Some(FileMeta {
					content: Some(content),
					content_type,
					content_disposition,
				}))
			},
			| _ => Ok(None),
		}
	}

	/// Gets all the MXC URIs in our media database
	pub async fn get_all_mxcs(&self) -> Result<Vec<OwnedMxcUri>> {
		let all_keys = self.db.get_all_media_keys().await;

		let mut mxcs = Vec::with_capacity(all_keys.len());

		for key in all_keys {
			trace!("Full MXC key from database: {key:?}");

			let mut parts = key.split(|&b| b == 0xFF);
			let mxc = parts
				.next()
				.map(|bytes| {
					utils::string_from_bytes(bytes).map_err(|e| {
						err!(Database(error!(
							"Failed to parse MXC unicode bytes from our database: {e}"
						)))
					})
				})
				.transpose()?;

			let Some(mxc_s) = mxc else {
				debug_warn!(
					mxc,
					"Parsed MXC URL unicode bytes from database but is still invalid"
				);
				continue;
			};

			trace!("Parsed MXC key to URL: {mxc_s}");
			let mxc = OwnedMxcUri::from(mxc_s);

			if mxc.is_valid() {
				mxcs.push(mxc);
			} else {
				debug_warn!("{mxc:?} from database was found to not be valid");
			}
		}

		Ok(mxcs)
	}

	/// Deletes all media files in the given time frame.
	/// Returns a usize with the amount of media files deleted.
	pub async fn delete_all_media_within_timeframe(
		&self,
		time_boundary: SystemTime,
		direction: TimeDirection,
		yes_i_want_to_delete_local_media: bool,
	) -> Result<usize> {
		let all_keys = self.db.get_all_media_keys().await;
		let mut candidates = Vec::with_capacity(all_keys.len());
		let mut seen_mxcs = HashSet::with_capacity(all_keys.len());

		for key in all_keys {
			trace!("Full MXC key from database: {key:?}");
			let mut parts = key.split(|&b| b == 0xFF);
			let mxc = parts
				.next()
				.map(|bytes| {
					utils::string_from_bytes(bytes).map_err(|e| {
						err!(Database(error!(
							"Failed to parse MXC unicode bytes from our database: {e}"
						)))
					})
				})
				.transpose()?;

			let Some(mxc_s) = mxc else {
				debug_warn!(
					mxc,
					"Parsed MXC URL unicode bytes from database but is still invalid"
				);
				continue;
			};

			trace!("Parsed MXC key to URL: {mxc_s}");
			let mxc = OwnedMxcUri::from(mxc_s);
			if (mxc.server_name() == Ok(self.services.globals.server_name())
				&& !yes_i_want_to_delete_local_media)
				|| !mxc.is_valid()
			{
				debug!("Ignoring local or broken media MXC: {mxc}");
				continue;
			}

			let mxc = mxc.to_string();
			if seen_mxcs.insert(mxc.clone()) {
				candidates.push(mxc);
			}
		}

		if candidates.is_empty() {
			return Err!(Database("Did not find any eligible MXCs to delete."));
		}

		debug_info!("Deleting media now {direction:?} {time_boundary:?}");

		let mut deletion_count: usize = 0;
		let mut found_eligible_mxc = false;

		for mxc in candidates {
			let mxc: Mxc<'_> = mxc.as_str().try_into()?;
			let _mxc_lock = self.mxc_mutex.lock(&mxc).await;
			let keys = self.db.search_mxc_metadata_prefix(&mxc).await;
			if keys.is_empty() {
				continue;
			}

			let mut within_bounds = false;
			for key in &keys {
				let modified_at = match self.store.created_at(key).await {
					| Ok(value) => value,
					| Err(error) if skip_media_inspection_error(self.store.is_local(), &error) => {
						error!("Skipping missing or inaccessible media object for MXC {mxc}: {error}");
						continue;
					},
					| Err(error) => {
						error!("Failed to inspect media for MXC {mxc}; stopping purge: {error}");
						return Err(error);
					},
				};

				debug!(%mxc, ?modified_at, "Media last-modified time");
				within_bounds |= time::is_within_bounds(modified_at, time_boundary, direction);
			}

			if !within_bounds {
				debug!("Media is outside bounds ({direction:?} {time_boundary:?}), ignoring.");
				continue;
			}

			found_eligible_mxc = true;

			debug_info!("Deleting MXC {mxc} from database and filesystem");

			match self.delete_keys_locked(&mxc, &keys).await {
				| Ok(()) => {
					deletion_count = deletion_count.saturating_add(1);
				},
				| Err(e) => {
					warn!("Failed to delete {mxc}: {e}");
					propagate_remote_media_storage_error(self.store.is_local(), e)?;
					continue;
				},
			}
		}

		if !found_eligible_mxc {
			return Err!(Database("Did not find any eligible MXCs to delete."));
		}

		Ok(deletion_count)
	}

	pub async fn create_media_dir(&self) -> Result<()> {
		self.store.initialize().await
	}

	pub fn is_local_storage(&self) -> bool {
		self.store.is_local()
	}

	#[inline]
	pub async fn get_metadata(&self, mxc: &Mxc<'_>) -> Option<FileMeta> {
		self.db
			.search_file_metadata(mxc, &Dim::default())
			.await
			.map(|metadata| FileMeta {
				content_disposition: metadata.content_disposition,
				content_type: metadata.content_type,
				content: None,
			})
			.ok()
	}

	#[inline]
	#[must_use]
	pub fn get_media_file(&self, key: &[u8]) -> PathBuf { self.get_media_file_sha256(key) }

	/// new SHA256 file name media function. requires database migrated. uses
	/// SHA256 hash of the base64 key as the file name
	#[must_use]
	pub fn get_media_file_sha256(&self, key: &[u8]) -> PathBuf {
		sha256_path(&self.get_media_dir(), key)
	}

	/// old base64 file name media function
	/// This is the old version of `get_media_file` that uses the full base64
	/// key as the filename.
	#[must_use]
	pub fn get_media_file_b64(&self, key: &[u8]) -> PathBuf {
		legacy_path(&self.get_media_dir(), key)
	}

	#[must_use]
	pub fn get_media_dir(&self) -> PathBuf {
		let mut r = PathBuf::new();
		r.push(self.services.server.config.database_path.clone());
		r.push("media");
		r
	}
}

#[inline]
#[must_use]
pub fn encode_key(key: &[u8]) -> String { general_purpose::URL_SAFE_NO_PAD.encode(key) }

fn propagate_remote_media_storage_error(
	is_local: bool,
	error: conduwuit::Error,
) -> Result<()> {
	if is_local { Ok(()) } else { Err(error) }
}

fn skip_media_inspection_error(is_local: bool, error: &conduwuit::Error) -> bool {
	is_local || error.is_not_found()
}

#[cfg(test)]
mod operation_lock_tests {
	#[cfg(feature = "s3")]
	use std::{
		any::Any,
		collections::BTreeMap,
		fmt,
		path::PathBuf,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
		time::{Duration, SystemTime, UNIX_EPOCH},
	};
	#[cfg(feature = "s3")]
	use async_trait::async_trait;
	#[cfg(feature = "s3")]
	use conduwuit::{Config, Server, config::Figment, log::Log};
	#[cfg(feature = "s3")]
	use futures::stream::BoxStream;
	#[cfg(feature = "s3")]
	use object_store::{
		CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
		PutMultipartOptions, PutOptions, PutPayload, PutResult,
		memory::InMemory,
		path::Path,
	};
	#[cfg(feature = "s3")]
	use ruma::OwnedMxcUri;
	#[cfg(feature = "s3")]
	use tokio::sync::{Notify, oneshot};

	use super::{propagate_remote_media_storage_error, skip_media_inspection_error};
	#[cfg(feature = "s3")]
	use super::{Data, Service, Services, mxc::Mxc, store};
	#[cfg(feature = "s3")]
	use super::Dim;
	#[cfg(feature = "s3")]
	use conduwuit::utils::time::TimeDirection;
	#[cfg(feature = "s3")]
	use crate::{Service as _, client, globals, moderation, sending};

	#[cfg(feature = "s3")]
	struct TestDirectory(PathBuf);

	#[cfg(feature = "s3")]
	impl TestDirectory {
		fn new() -> Self {
			let nonce = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system clock should be after Unix epoch")
				.as_nanos();
			Self(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
				"continuwuity-media-race-{}-{nonce}",
				std::process::id()
			)))
		}
	}

	#[cfg(feature = "s3")]
	impl Drop for TestDirectory {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}

	#[cfg(feature = "s3")]
	#[derive(Debug)]
	struct InstrumentedStore {
		inner: InMemory,
		pause_next_put: AtomicBool,
		put_started: Notify,
		resume_put: Notify,
		fail_next_head: AtomicBool,
	}

	#[cfg(feature = "s3")]
	impl InstrumentedStore {
		fn new() -> Self {
			Self {
				inner: InMemory::new(),
				pause_next_put: AtomicBool::new(false),
				put_started: Notify::new(),
				resume_put: Notify::new(),
				fail_next_head: AtomicBool::new(false),
			}
		}
	}

	#[cfg(feature = "s3")]
	impl fmt::Display for InstrumentedStore {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("InstrumentedStore") }
	}

	#[cfg(feature = "s3")]
	#[async_trait]
	impl ObjectStore for InstrumentedStore {
		async fn put_opts(
			&self,
			location: &Path,
			payload: PutPayload,
			options: PutOptions,
		) -> object_store::Result<PutResult> {
			if self.pause_next_put.swap(false, Ordering::AcqRel) {
				self.put_started.notify_one();
				self.resume_put.notified().await;
			}

			self.inner.put_opts(location, payload, options).await
		}

		async fn put_multipart_opts(
			&self,
			location: &Path,
			options: PutMultipartOptions,
		) -> object_store::Result<Box<dyn MultipartUpload>> {
			self.inner.put_multipart_opts(location, options).await
		}

		async fn get_opts(
			&self,
			location: &Path,
			options: GetOptions,
		) -> object_store::Result<GetResult> {
			if options.head && self.fail_next_head.swap(false, Ordering::AcqRel) {
				return Err(object_store::Error::PermissionDenied {
					path: location.to_string(),
					source: Box::new(std::io::Error::new(
						std::io::ErrorKind::PermissionDenied,
						"injected S3 inspection denial",
					)),
				});
			}

			self.inner.get_opts(location, options).await
		}

		fn delete_stream(
			&self,
			locations: BoxStream<'static, object_store::Result<Path>>,
		) -> BoxStream<'static, object_store::Result<Path>> {
			self.inner.delete_stream(locations)
		}

		fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
			self.inner.list(prefix)
		}

		async fn list_with_delimiter(
			&self,
			prefix: Option<&Path>,
		) -> object_store::Result<ListResult> {
			self.inner.list_with_delimiter(prefix).await
		}

		async fn copy_opts(
			&self,
			from: &Path,
			to: &Path,
			options: CopyOptions,
		) -> object_store::Result<()> {
			self.inner.copy_opts(from, to, options).await
		}
	}

	#[cfg(feature = "s3")]
	fn test_server(database_path: PathBuf) -> Arc<Server> {
		let raw = Figment::new()
			.merge(("server_name", "example.org"))
			.merge(("database_path", database_path.to_string_lossy().into_owned()));
		let config = Config::new(&raw).expect("test server configuration should deserialize");
		Arc::new(Server::new(
			config,
			None,
			Log {
				reload: Default::default(),
				capture: Arc::new(conduwuit::log::capture::State::new()),
			},
		))
	}

	#[cfg(feature = "s3")]
	async fn test_media_service(
		directory: &TestDirectory,
		store: Arc<InstrumentedStore>,
	) -> (
		Arc<database::Database>,
		Arc<Service>,
		Arc<Server>,
		Arc<crate::service::Map>,
		Arc<globals::Service>,
	) {
		let server = test_server(directory.0.join("database"));
		let database = database::Database::open(&server)
			.await
			.expect("temporary test database should open");
		let service_map = Arc::new(crate::service::Map::new(BTreeMap::new()));
		let args = || crate::Args {
			db: &database,
			server: &server,
			service: &service_map,
		};

		let globals_service = globals::Service::build(args())
			.expect("globals service should build for the test server");
		let globals_as_service: Arc<dyn crate::Service> = globals_service.clone();
		let globals_as_any: Arc<dyn Any + Send + Sync> = globals_service.clone();
		service_map.write().insert(
			crate::Service::name(globals_service.as_ref()).to_owned(),
			(Arc::downgrade(&globals_as_service), Arc::downgrade(&globals_as_any)),
		);

		let args = args();
		let service = Arc::new(Service {
			url_preview_mutex: Default::default(),
			mxc_mutex: Default::default(),
			db: Data::new(&database),
			store: store::for_test_store(store),
			services: Services {
				server: server.clone(),
				client: args.depend::<client::Service>("client"),
				globals: args.depend::<globals::Service>("globals"),
				sending: args.depend::<sending::Service>("sending"),
				moderation: args.depend::<moderation::Service>("moderation"),
			},
		});

		(database, service, server, service_map, globals_service)
	}

	#[cfg(feature = "s3")]
	#[tokio::test]
	async fn a_delete_waiting_on_an_upload_leaves_no_stale_metadata_or_object() {
		let temp = TestDirectory::new();
		let store = Arc::new(InstrumentedStore::new());
		let (_database, service, _server, _service_map, _globals) =
			test_media_service(&temp, Arc::clone(&store)).await;
		let uri = OwnedMxcUri::from("mxc://remote.example/same-media");
		let mxc = Mxc::try_from(&uri).expect("fixture MXC URI should parse");
		service
			.create(&mxc, None, None, Some("application/octet-stream"), b"original bytes")
			.await
			.expect("initial upload should succeed");

		store.pause_next_put.store(true, Ordering::Release);
		let upload_service = Arc::clone(&service);
		let upload_uri = uri.clone();
		let upload = tokio::spawn(async move {
			let mxc = Mxc::try_from(&upload_uri).expect("fixture MXC URI should parse");
			upload_service
				.create(&mxc, None, None, Some("application/octet-stream"), b"replacement bytes")
				.await
		});
		store.put_started.notified().await;

		let delete_service = Arc::clone(&service);
		let delete_uri = uri.clone();
		let (delete_started_tx, delete_started_rx) = oneshot::channel();
		let mut delete = tokio::spawn(async move {
			delete_started_tx.send(()).expect("delete receiver should be alive");
			let mxc = Mxc::try_from(&delete_uri).expect("fixture MXC URI should parse");
			delete_service.delete(&mxc).await
		});
		delete_started_rx.await.expect("delete should start");
		assert!(
			tokio::time::timeout(Duration::from_millis(50), &mut delete)
				.await
				.is_err(),
			"delete must wait for the in-flight upload holding the real MXC lock"
		);

		store.resume_put.notify_one();
		upload.await.expect("upload task should finish").expect("replacement upload should succeed");
		delete
			.await
			.expect("delete task should finish")
			.expect("delete should finish after upload");
		let upload_key = service
			.db
			.prepare_file_metadata(&mxc, &Dim::default(), None, Some("application/octet-stream"))
			.expect("media metadata key should be prepared");
		assert!(service.store.get(&upload_key).await.is_err(), "deleted S3 object should be absent");
		assert!(service.get(&mxc).await.expect("media lookup should succeed").is_none());

		let purge_uri = OwnedMxcUri::from("mxc://remote.example/purge-media");
		let purge_mxc = Mxc::try_from(&purge_uri).expect("fixture MXC URI should parse");
		service
			.create(&purge_mxc, None, None, Some("application/octet-stream"), b"purge bytes")
			.await
			.expect("purge fixture upload should succeed");
		store.fail_next_head.store(true, Ordering::Release);
		let purge_error = service
			.delete_all_media_within_timeframe(
				SystemTime::now() + Duration::from_secs(60),
				TimeDirection::Before,
				false,
			)
			.await
			.expect_err("S3 inspection failures must stop the purge");
		assert!(purge_error.to_string().contains("permission denied by S3"));
		assert!(
			service
				.get(&purge_mxc)
				.await
				.expect("metadata should remain readable after a failed purge")
				.is_some(),
			"failed purge must leave the object metadata intact"
		);
		service.delete(&purge_mxc).await.expect("purge fixture cleanup should succeed");
	}

	#[test]
	fn timeframe_purge_skips_missing_objects_but_not_other_s3_errors() {
		let missing: conduwuit::Error =
			std::io::Error::new(std::io::ErrorKind::NotFound, "object missing").into();
		assert!(skip_media_inspection_error(false, &missing));

		let denied: conduwuit::Error =
			std::io::Error::new(std::io::ErrorKind::PermissionDenied, "S3 denied").into();
		assert!(!skip_media_inspection_error(false, &denied));
		assert!(skip_media_inspection_error(true, &denied));
	}

	#[test]
	fn remote_media_delete_errors_are_propagated_and_local_errors_remain_best_effort() {
		let s3_error: conduwuit::Error = conduwuit::err!("S3 request failed");
		let returned = propagate_remote_media_storage_error(false, s3_error)
			.expect_err("S3 media errors must reach the admin command");
		assert!(returned.to_string().contains("S3 request failed"));

		let local_error: conduwuit::Error = conduwuit::err!("local media file is missing");
		assert!(propagate_remote_media_storage_error(true, local_error).is_ok());
	}
}
