use std::{
	path::{Path, PathBuf},
	time::SystemTime,
};

use conduwuit::{Config, Err, Result, debug_error};
use tokio::fs;

use super::encode_key;

#[cfg(feature = "s3")]
use conduwuit::err;

#[cfg(feature = "s3")]
use std::time::Duration;

#[cfg(feature = "s3")]
use object_store::ObjectStoreExt;

pub(super) enum MediaStore {
	Local {
		root: PathBuf,
		compatibility_links: bool,
	},
	#[cfg(feature = "s3")]
	S3(S3MediaStore),
}

impl MediaStore {
	pub(super) fn build(config: &Config) -> Result<Self> {
		match config.media_backend {
			| conduwuit::config::MediaBackend::Local => Ok(Self::Local {
				root: config.database_path.join("media"),
				compatibility_links: config.media_compat_file_link,
			}),
			| conduwuit::config::MediaBackend::S3 => {
				#[cfg(feature = "s3")]
				{
					let config = config.media_s3.as_ref().ok_or_else(|| {
						err!(Config(
							"media_s3",
							"S3 settings are required when media_backend = \"s3\""
						))
					})?;
					Ok(Self::S3(S3MediaStore::build(config)?))
				}

				#[cfg(not(feature = "s3"))]
				{
					Err!(Config(
						"media_backend",
						"S3 media storage was selected, but this binary was built without the `s3` feature"
					))
				}
			},
		}
	}

	pub(super) fn is_local(&self) -> bool {
		matches!(self, Self::Local { .. })
	}

	pub(super) async fn initialize(&self) -> Result<()> {
		match self {
			| Self::Local { root, .. } => fs::create_dir_all(root).await?,
			#[cfg(feature = "s3")]
			| Self::S3(_) => {},
		}

		Ok(())
	}

	pub(super) async fn put(&self, key: &[u8], content: &[u8]) -> Result<()> {
		match self {
			| Self::Local { root, compatibility_links } => {
				let path = sha256_path(root, key);
				fs::write(&path, content).await?;

				if *compatibility_links {
					let legacy = legacy_path(root, key);
					if let Err(error) = fs::symlink(&path, &legacy).await {
						debug_error!(
							?key,
							?path,
							?legacy,
							"Failed to create legacy media symlink: {error}"
						);
					}
				}

				Ok(())
			},
			#[cfg(feature = "s3")]
			| Self::S3(store) => {
				let location = store.location(key)?;
				store
					.store
					.put(&location, bytes::Bytes::copy_from_slice(content).into())
					.await
					.map_err(|error| object_store_error("write", key, error))?;

				Ok(())
			},
		}
	}

	pub(super) async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
		match self {
			| Self::Local { root, .. } => Ok(fs::read(sha256_path(root, key)).await?),
			#[cfg(feature = "s3")]
			| Self::S3(store) => {
				let location = store.location(key)?;
				let result = store
					.store
					.get(&location)
					.await
					.map_err(|error| object_store_error("read", key, error))?;
				let bytes = result
					.bytes()
					.await
					.map_err(|error| object_store_error("read", key, error))?;
				Ok(bytes.into())
			},
		}
	}

	pub(super) async fn delete(&self, key: &[u8]) -> Result<()> {
		match self {
			| Self::Local { root, compatibility_links } => {
				let path = sha256_path(root, key);
				let primary_error = match fs::remove_file(&path).await {
					| Ok(()) => None,
					| Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
					| Err(error) => Some(error),
				};

				let legacy = legacy_path(root, key);
				if let Err(error) = fs::remove_file(&legacy).await {
					if *compatibility_links && error.kind() != std::io::ErrorKind::NotFound {
						debug_error!(
							?key,
							?legacy,
							"Failed to remove legacy media symlink: {error}"
						);
					}
				}

				match primary_error {
					| Some(error) => Err(error.into()),
					| None => Ok(()),
				}
			},
			#[cfg(feature = "s3")]
			| Self::S3(store) => {
				let location = store.location(key)?;
				match store.store.delete(&location).await {
					| Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
					| Err(error) => Err(object_store_error("delete", key, error).into()),
				}
			},
		}
	}

	pub(super) async fn created_at(&self, key: &[u8]) -> Result<SystemTime> {
		match self {
			| Self::Local { root, .. } => {
				let metadata = fs::metadata(sha256_path(root, key)).await?;
				match metadata.created() {
					| Ok(created) => Ok(created),
					| Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
						Ok(metadata.modified()?)
					},
					| Err(error) => Err(error.into()),
				}
			},
			#[cfg(feature = "s3")]
			| Self::S3(store) => {
				let location = store.location(key)?;
				let metadata = store
					.store
					.head(&location)
					.await
					.map_err(|error| object_store_error("inspect", key, error))?;

				let timestamp = metadata.last_modified;
				let seconds = u64::try_from(timestamp.timestamp()).map_err(|error| {
					std::io::Error::other(format!("Invalid S3 last-modified timestamp: {error}"))
				})?;
				SystemTime::UNIX_EPOCH
					.checked_add(Duration::new(seconds, timestamp.timestamp_subsec_nanos()))
					.ok_or_else(|| {
						std::io::Error::other("S3 last-modified timestamp is out of range").into()
					})
			},
		}
	}
}

pub(super) fn sha256_path(root: &Path, key: &[u8]) -> PathBuf {
	let digest = <sha2::Sha256 as sha2::Digest>::digest(key);
	root.join(encode_key(&digest))
}

pub(super) fn legacy_path(root: &Path, key: &[u8]) -> PathBuf {
	root.join(encode_key(key))
}

#[cfg(test)]
mod local_path_tests {
	use std::path::Path;

	#[cfg(unix)]
	use std::{
		path::PathBuf,
		time::{SystemTime, UNIX_EPOCH},
	};

	#[cfg(unix)]
	use super::MediaStore;
	use super::{legacy_path, sha256_path};
	#[cfg(unix)]
	use tokio::fs;

	#[cfg(unix)]
	struct TestDirectory(PathBuf);

	#[cfg(unix)]
	impl TestDirectory {
		fn new() -> Self {
			let nonce = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system clock should be after Unix epoch")
				.as_nanos();
			Self(std::env::temp_dir().join(format!(
				"continuwuity-media-store-{}-{nonce}",
				std::process::id()
			)))
		}
	}

	#[cfg(unix)]
	impl Drop for TestDirectory {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}

	#[test]
	fn local_media_paths_keep_the_existing_sha256_and_legacy_names() {
		let root = Path::new("/var/lib/continuwuity/media");
		let key = b"media-key-fixture";

		assert_eq!(
			sha256_path(root, key),
			root.join("9kM8BRJbMztWCbtt6yBea6VcVjUGMKwwgPplElYRZ90")
		);
		assert_eq!(legacy_path(root, key), root.join("bWVkaWEta2V5LWZpeHR1cmU"));
	}

	#[cfg(unix)]
	#[tokio::test]
	async fn local_store_keeps_sha256_files_and_optional_legacy_symlinks_compatible() {
		let temp = TestDirectory::new();
		let root = temp.0.join("media");
		let compatibility_store = MediaStore::Local {
			root: root.clone(),
			compatibility_links: true,
		};
		let default_store = MediaStore::Local {
			root: root.clone(),
			compatibility_links: false,
		};
		compatibility_store
			.initialize()
			.await
			.expect("media directory should initialize");

		let original_key = b"mxc://example.org/original";
		let thumbnail_key = b"mxc://example.org/thumbnail";
		let original = b"original image bytes";
		let thumbnail = b"thumbnail image bytes";
		compatibility_store
			.put(original_key, original)
			.await
			.expect("original media should be written");
		default_store
			.put(thumbnail_key, thumbnail)
			.await
			.expect("thumbnail media should be written");

		let original_path = sha256_path(&root, original_key);
		let original_legacy_path = legacy_path(&root, original_key);
		assert_eq!(fs::read(&original_path).await.unwrap(), original.to_vec());
		assert_eq!(fs::read(&original_legacy_path).await.unwrap(), original.to_vec());
		assert!(
			fs::symlink_metadata(&original_legacy_path)
				.await
			.unwrap()
				.file_type()
				.is_symlink(),
			"the legacy name should remain a symlink to the SHA256 file"
		);
		assert_eq!(compatibility_store.get(original_key).await.unwrap(), original.to_vec());

		let thumbnail_path = sha256_path(&root, thumbnail_key);
		let thumbnail_legacy_path = legacy_path(&root, thumbnail_key);
		assert!(fs::symlink_metadata(&thumbnail_legacy_path).await.is_err());
		assert_eq!(default_store.get(thumbnail_key).await.unwrap(), thumbnail.to_vec());
		default_store
			.delete(thumbnail_key)
			.await
			.expect("thumbnail media should be deleted");
		assert!(fs::metadata(&thumbnail_path).await.is_err());
		assert!(fs::symlink_metadata(&thumbnail_legacy_path).await.is_err());
		assert_eq!(
			compatibility_store.get(original_key).await.unwrap(),
			original.to_vec(),
			"deleting one media key must leave a distinct original untouched"
		);
	}
}

#[cfg(feature = "s3")]
pub(super) struct S3MediaStore {
	store: std::sync::Arc<dyn object_store::ObjectStore>,
	key_prefix: String,
}

#[cfg(feature = "s3")]
impl S3MediaStore {
	fn build(config: &conduwuit::config::MediaS3Config) -> Result<Self> {
		use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

		let bucket = config.bucket.as_ref().ok_or_else(|| {
			err!(Config(
				"media_s3.bucket",
				"A bucket name is required when media_backend = \"s3\""
			))
		})?;

		let mut builder = AmazonS3Builder::from_env()
			.with_bucket_name(bucket)
			.with_allow_http(config.allow_http);
		let static_credentials = match (&config.access_key_id, &config.secret_access_key) {
			| (Some(access_key_id), Some(secret_access_key)) => Some(
				object_store::aws::AwsCredential {
					key_id: access_key_id.expose_secret().to_owned(),
					secret_key: secret_access_key.expose_secret().to_owned(),
					token: config
						.session_token
						.as_ref()
						.map(|token| token.expose_secret().to_owned()),
				},
			),
			| (None, None) if config.session_token.is_none() => None,
			| _ => {
				return Err!(Config(
					"media_s3.access_key_id",
					"Static S3 credentials require both access_key_id and secret_access_key"
				));
			},
		};
		if let Some(credentials) = static_credentials {
			// Use only the configured static tuple. In particular, don't mix an
			// unrelated AWS_SESSION_TOKEN from the process environment into it.
			builder = builder.with_credentials(std::sync::Arc::new(
				object_store::StaticCredentialProvider::new(credentials),
			));
		}
		if let Some(region) = &config.region {
			builder = builder.with_region(region);
		}
		if let Some(endpoint) = &config.endpoint {
			builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
		}
		if let Some(virtual_hosted_style_request) = config.virtual_hosted_style_request {
			builder = builder.with_virtual_hosted_style_request(virtual_hosted_style_request);
		}
		let key_prefix = config.key_prefix.trim_matches('/').to_owned();
		if !key_prefix.is_empty() {
			object_store::path::Path::parse(&key_prefix).map_err(|error| {
				err!(Config("media_s3.key_prefix", "Invalid S3 object key prefix: {error}"))
			})?;
		}

		let store = builder.build().map_err(|_| {
			err!(Config(
				"media_s3",
				"Failed to initialize S3 media storage; check the bucket, endpoint, region, and credentials"
			))
		})?;

		Ok(Self {
			store: std::sync::Arc::new(store),
			key_prefix,
		})
	}

	fn location(&self, key: &[u8]) -> Result<object_store::path::Path> {
		let digest = <sha2::Sha256 as sha2::Digest>::digest(key);
		let file_name = encode_key(&digest);
		let object_key = if self.key_prefix.is_empty() {
			file_name
		} else {
			format!("{}/{file_name}", self.key_prefix)
		};

		object_store::path::Path::parse(object_key).map_err(|error| {
			std::io::Error::other(format!("Invalid generated S3 media object key: {error}"))
				.into()
		})
	}
}

#[cfg(all(test, feature = "s3"))]
pub(super) fn for_test_store(store: std::sync::Arc<dyn object_store::ObjectStore>) -> MediaStore {
	MediaStore::S3(S3MediaStore { store, key_prefix: String::new() })
}

#[cfg(feature = "s3")]
fn object_store_error(operation: &str, key: &[u8], error: object_store::Error) -> std::io::Error {
	let (kind, summary) = match &error {
		| object_store::Error::NotFound { .. } => {
			(std::io::ErrorKind::NotFound, "object not found")
		},
		| object_store::Error::PermissionDenied { .. } => {
			(std::io::ErrorKind::Other, "permission denied by S3")
		},
		| object_store::Error::Unauthenticated { .. } => {
			(std::io::ErrorKind::Other, "S3 authentication failed")
		},
		| object_store::Error::InvalidPath { .. } => {
			(std::io::ErrorKind::Other, "invalid object path")
		},
		| _ => (std::io::ErrorKind::Other, "storage request failed"),
	};
	let digest = <sha2::Sha256 as sha2::Digest>::digest(key);
	let object_key = encode_key(&digest);
	std::io::Error::new(
		kind,
		format!("S3 media {operation} failed for object {object_key}: {summary}"),
	)
}

#[cfg(all(test, feature = "s3"))]
mod tests {
	use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};

	use object_store::{ObjectStoreExt, memory::InMemory};

	use super::{MediaStore, S3MediaStore, object_store_error};

	#[tokio::test]
	async fn s3_adapter_roundtrips_original_and_thumbnail_and_deletes_only_the_requested_object() {
		let media = MediaStore::S3(S3MediaStore {
			store: Arc::new(InMemory::new()),
			key_prefix: "tenant/media".to_owned(),
		});
		let original_key = b"mxc://example.org/original";
		let thumbnail_key = b"mxc://example.org/thumbnail";
		let path = match &media {
			| MediaStore::S3(store) => store
				.location(original_key)
				.expect("fixture key should be valid"),
			| MediaStore::Local { .. } => unreachable!(),
		};

		assert!(path.to_string().starts_with("tenant/media/"));
		assert_eq!(
			path,
			match &media {
				| MediaStore::S3(store) => store
					.location(original_key)
					.expect("same key should stay valid"),
				| MediaStore::Local { .. } => unreachable!(),
			}
		);
		let thumbnail_path = match &media {
			| MediaStore::S3(store) => store
				.location(thumbnail_key)
				.expect("thumbnail key should be valid"),
			| MediaStore::Local { .. } => unreachable!(),
		};
		assert_ne!(path, thumbnail_path, "distinct media keys need distinct objects");

		let original = b"original image bytes";
		let thumbnail = b"thumbnail image bytes";
		media
			.put(original_key, original)
			.await
			.expect("original upload should work");
		media
			.put(thumbnail_key, thumbnail)
			.await
			.expect("thumbnail upload should work");
		assert_eq!(
			media.get(original_key).await.expect("original download should work"),
			original.to_vec()
		);
		assert_eq!(
			media
				.get(thumbnail_key)
				.await
				.expect("thumbnail download should work"),
			thumbnail.to_vec()
		);
		assert!(
			media
				.created_at(original_key)
				.await
				.expect("in-memory head should work")
				<= SystemTime::now()
		);

		media
			.delete(thumbnail_key)
			.await
			.expect("thumbnail deletion should work");
		assert!(media.get(thumbnail_key).await.is_err());
		assert_eq!(
			media.get(original_key).await.expect("original should remain"),
			original.to_vec()
		);
		media
			.delete(thumbnail_key)
			.await
			.expect("deleting a missing S3 object should be idempotent");
	}

	#[tokio::test]
	async fn missing_object_maps_to_not_found_without_exposing_the_input_key() {
		let store = S3MediaStore {
			store: Arc::new(InMemory::new()),
			key_prefix: String::new(),
		};
		let key = b"private-media-key";
		let path = store.location(key).expect("fixture key should be valid");
		let source = match store.store.get(&path).await {
			| Ok(_) => panic!("empty in-memory store unexpectedly contained the fixture"),
			| Err(error) => error,
		};

		let error = object_store_error("read", key, source);
		assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
		assert!(error.to_string().contains("object not found"));
		assert!(!error.to_string().contains("private-media-key"));
	}

	#[tokio::test]
	async fn missing_s3_object_reports_not_found_for_timeframe_selection() {
		let media = MediaStore::S3(S3MediaStore {
			store: Arc::new(InMemory::new()),
			key_prefix: "tenant/media".to_owned(),
		});
		let error = media
			.created_at(b"missing-media-key")
			.await
			.expect_err("missing object should have no timestamp");

		assert!(error.is_not_found());
	}

	#[test]
	fn provider_errors_are_generic_and_do_not_expose_provider_details() {
		let source = object_store::Error::PermissionDenied {
			path: "private-media-key".to_owned(),
			source: Box::new(std::io::Error::other("token=provider-secret")),
		};
		let error = object_store_error("delete", b"private-media-key", source);
		let message = error.to_string();

		assert_eq!(error.kind(), std::io::ErrorKind::Other);
		assert!(message.contains("permission denied by S3"));
		assert!(!message.contains("provider-secret"));
		assert!(!message.contains("private-media-key"));
	}

	#[tokio::test]
	#[ignore = "requires a disposable S3 endpoint/bucket and AWS environment credentials"]
	async fn configured_s3_endpoint_roundtrips_and_deletes_one_object() {
		// The server binary installs the TLS provider at startup; this service crate test
		// bypasses that path. Keep an already-installed provider if another test set one.
		let _ = rustls::crypto::ring::default_provider().install_default();

		let endpoint = std::env::var("CONDUWUIT_S3_TEST_ENDPOINT")
			.expect("set CONDUWUIT_S3_TEST_ENDPOINT to an S3-compatible endpoint");
		let bucket = std::env::var("CONDUWUIT_S3_TEST_BUCKET")
			.expect("set CONDUWUIT_S3_TEST_BUCKET to a disposable bucket");
		let allow_http = std::env::var("CONDUWUIT_S3_TEST_ALLOW_HTTP")
			.is_ok_and(|value| value.eq_ignore_ascii_case("true"));
		let config = conduwuit::config::MediaS3Config {
			bucket: Some(bucket),
			endpoint: Some(endpoint),
			allow_http,
			..Default::default()
		};
		let media = MediaStore::S3(
			S3MediaStore::build(&config).expect("S3 settings should construct the backend"),
		);
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock should be after Unix epoch")
			.as_nanos();
		let key = format!("s3-roundtrip:{}:{nonce}", std::process::id()).into_bytes();
		let content = b"conduwuit s3 smoke test";
		let transfer = async {
			media.put(&key, content).await?;
			media.get(&key).await
		}
		.await;
		let cleanup = media.delete(&key).await;

		assert!(cleanup.is_ok(), "smoke-test object cleanup failed");
		assert_eq!(transfer.expect("S3 upload/download should succeed"), content.to_vec());
	}
}
