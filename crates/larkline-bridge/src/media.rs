//! Inbound message attachments: download, policy-check and cache.
//!
//! Port of `src/media/attachment.ts` and `src/media/cache.ts`. A resource
//! descriptor on an inbound message is downloaded through the SDK's
//! authenticated `im/v1/messages/<id>/resources/<key>` endpoint, written to
//! `<profile>/media/` under a content-hash name, and reported to the prompt as
//! an attachment. Downloads are strictly best-effort: a failed attachment is
//! logged and skipped, never propagated as a run failure.
//!
//! ## Cache policy
//!
//! * File names are `<sha256>.<ext>`, where the extension comes from a small
//!   allow-list keyed by MIME type (`bin` for everything else). The original
//!   file name is kept only as metadata, never used as a path, so an upstream
//!   name can never traverse directories.
//! * Writes are atomic: bytes go to a `.tmp-<pid>-<uuid>` file in the same
//!   directory and are renamed into place. A concurrent writer of the same
//!   content is treated as a cache hit.
//! * After every resolve the cache evicts entries older than the TTL, then
//!   enforces the entry-count and byte caps (oldest first), always keeping the
//!   attachments accepted by the current run.
//!
//! ## Attachment policy
//!
//! The caps mirror the TypeScript defaults (`maxCount` 10, `maxBytes` 100 MiB,
//! `maxFileBytes` 25 MiB, `imageMaxBytes` 25 MiB, 24h TTL, 512 MiB cache) and
//! can be overridden through `profile.extra.attachments` with the same
//! camelCase keys the TS profile schema writes.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use lark_channel::media::{DownloadedResource, ResourceDescriptor, ResourceType};
use larkline_agent::PromptAttachment;
use larkline_core::{AppPaths, ProfileConfig};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;

/// Per-run attachment caps, plus the on-disk cache bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaPolicy {
    /// Maximum attachments considered per run (`maxCount`).
    pub max_count: usize,
    /// Maximum bytes accepted across one run (`maxBytes`).
    pub max_bytes: u64,
    /// Maximum bytes for any one file (`maxFileBytes`).
    pub max_file_bytes: u64,
    /// Maximum bytes for any one image (`imageMaxBytes`).
    pub image_max_bytes: u64,
    /// Files not modified within this window are evicted (`cacheTtlMs`).
    pub cache_ttl: Duration,
    /// Soft byte cap for the whole cache (`cacheMaxBytes`).
    pub cache_max_bytes: u64,
    /// Rust-only entry-count cap for the cache directory.
    pub cache_max_files: usize,
}

impl Default for MediaPolicy {
    fn default() -> Self {
        Self {
            max_count: 10,
            max_bytes: 100 * 1024 * 1024,
            max_file_bytes: 25 * 1024 * 1024,
            image_max_bytes: 25 * 1024 * 1024,
            cache_ttl: Duration::from_secs(24 * 60 * 60),
            cache_max_bytes: 512 * 1024 * 1024,
            cache_max_files: 1024,
        }
    }
}

impl MediaPolicy {
    /// Read `profile.extra.attachments`, falling back to the TS defaults.
    ///
    /// Unknown or non-numeric values are ignored individually, like the TS
    /// `numberOr` normalizer, so one bad key cannot disable attachments.
    pub fn from_profile(profile: &ProfileConfig) -> Self {
        let mut policy = Self::default();
        let Some(raw) = profile.extra.get("attachments") else {
            return policy;
        };
        if let Some(value) = number(raw, "maxCount") {
            policy.max_count = usize::try_from(value).unwrap_or(usize::MAX);
        }
        if let Some(value) = number(raw, "maxBytes") {
            policy.max_bytes = value;
        }
        if let Some(value) = number(raw, "maxFileBytes") {
            policy.max_file_bytes = value;
        }
        if let Some(value) = number(raw, "imageMaxBytes") {
            policy.image_max_bytes = value;
        }
        if let Some(value) = number(raw, "cacheTtlMs") {
            policy.cache_ttl = Duration::from_millis(value);
        }
        if let Some(value) = number(raw, "cacheMaxBytes") {
            policy.cache_max_bytes = value;
        }
        // Rust-only extension: the TS cache has no entry-count cap.
        if let Some(value) = number(raw, "cacheMaxFiles") {
            policy.cache_max_files = usize::try_from(value).unwrap_or(usize::MAX);
        }
        policy
    }
}

fn number(raw: &Value, key: &str) -> Option<u64> {
    raw.get(key).and_then(Value::as_u64)
}

/// Attachment kind reported to the prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttachmentKind {
    Image,
    File,
    Audio,
    Video,
    Sticker,
}

impl AttachmentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Sticker => "sticker",
        }
    }

    /// Map an SDK resource type to the TS attachment kind.
    ///
    /// `Folder` and `Unknown` have no TS attachment kind and are skipped by
    /// the resolver before any download is attempted.
    pub fn from_resource_type(resource_type: ResourceType) -> Option<Self> {
        match resource_type {
            ResourceType::Image => Some(Self::Image),
            ResourceType::File => Some(Self::File),
            ResourceType::Audio => Some(Self::Audio),
            // TS normalizes a `media` message to attachment kind `video`.
            ResourceType::Media => Some(Self::Video),
            ResourceType::Sticker => Some(Self::Sticker),
            ResourceType::Folder | ResourceType::Unknown => None,
        }
    }

    pub fn default_mime(self) -> &'static str {
        match self {
            Self::Image => "image/png",
            Self::Audio => "audio/ogg",
            Self::Video => "video/mp4",
            Self::File | Self::Sticker => "application/octet-stream",
        }
    }
}

/// The attachment policy decision (`AttachmentDecision` in TS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentDecision {
    Accepted,
    Rejected,
    Skipped,
}

impl AttachmentDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Skipped => "skipped",
        }
    }
}

/// `requiredness` on an inbound attachment. Message resources are always
/// optional, exactly like TS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentRequiredness {
    Required,
    Optional,
}

impl AttachmentRequiredness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Optional => "optional",
        }
    }
}

/// A downloaded resource before the per-run policy is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentCandidate {
    pub abs_path: PathBuf,
    pub kind: AttachmentKind,
    pub size: u64,
    pub mime: String,
    pub hash: String,
    pub source_message_id: String,
    pub source_file_key: String,
    pub original_name: Option<String>,
}

/// A candidate plus its policy decision and prompt-facing path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedAttachment {
    pub path: PathBuf,
    pub kind: AttachmentKind,
    pub size: u64,
    pub mime: String,
    pub hash: String,
    pub source_message_id: String,
    pub source_file_key: String,
    pub original_name: Option<String>,
    pub requiredness: AttachmentRequiredness,
    pub decision: AttachmentDecision,
    pub rejection_reason: Option<String>,
}

impl NormalizedAttachment {
    pub fn is_accepted(&self) -> bool {
        self.decision == AttachmentDecision::Accepted
    }

    pub fn is_image(&self) -> bool {
        self.kind == AttachmentKind::Image
    }

    /// Prompt entry for this attachment, matching the TS JSON shape.
    pub fn to_prompt_attachment(&self) -> PromptAttachment {
        PromptAttachment {
            path: self.path.to_string_lossy().into_owned(),
            kind: self.kind.as_str().to_string(),
            hash: Some(self.hash.clone()),
            size: Some(self.size),
            mime: Some(self.mime.clone()),
            source_message_id: Some(self.source_message_id.clone()),
            requiredness: Some(self.requiredness.as_str().to_string()),
            decision: Some(self.decision.as_str().to_string()),
            rejection_reason: self.rejection_reason.clone(),
        }
    }
}

/// Convert a resolve result into the prompt section payload.
pub fn to_prompt_attachments(attachments: &[NormalizedAttachment]) -> Vec<PromptAttachment> {
    attachments
        .iter()
        .map(NormalizedAttachment::to_prompt_attachment)
        .collect()
}

/// Apply the per-run attachment policy (port of `normalizeAttachments`).
///
/// Order is significant: candidates are considered in arrival order, the
/// early kinds (sticker/audio/video/unsupported image MIME) never consume the
/// count or byte budget, and the first cap hit rejects only the candidates
/// after it.
pub fn normalize_attachments(
    candidates: &[AttachmentCandidate],
    policy: &MediaPolicy,
) -> Vec<NormalizedAttachment> {
    let mut accepted_count = 0usize;
    let mut accepted_bytes: u64 = 0;

    candidates
        .iter()
        .map(|candidate| {
            let mut attachment = NormalizedAttachment {
                path: candidate.abs_path.clone(),
                kind: candidate.kind,
                size: candidate.size,
                mime: candidate.mime.clone(),
                hash: candidate.hash.clone(),
                source_message_id: candidate.source_message_id.clone(),
                source_file_key: candidate.source_file_key.clone(),
                original_name: candidate.original_name.clone(),
                requiredness: AttachmentRequiredness::Optional,
                decision: AttachmentDecision::Accepted,
                rejection_reason: None,
            };

            if let Some((decision, reason)) = early_decision(candidate) {
                attachment.decision = decision;
                attachment.rejection_reason = Some(reason.to_string());
                return attachment;
            }

            let rejection = if accepted_count >= policy.max_count {
                Some("too-many-attachments")
            } else if candidate.size > policy.max_file_bytes {
                Some("file-too-large")
            } else if candidate.kind == AttachmentKind::Image
                && candidate.size > policy.image_max_bytes
            {
                Some("image-too-large")
            } else if accepted_bytes.saturating_add(candidate.size) > policy.max_bytes {
                Some("run-too-large")
            } else {
                None
            };

            match rejection {
                Some(reason) => {
                    attachment.decision = AttachmentDecision::Rejected;
                    attachment.rejection_reason = Some(reason.to_string());
                }
                None => {
                    accepted_count += 1;
                    accepted_bytes = accepted_bytes.saturating_add(candidate.size);
                }
            }
            attachment
        })
        .collect()
}

fn early_decision(candidate: &AttachmentCandidate) -> Option<(AttachmentDecision, &'static str)> {
    match candidate.kind {
        AttachmentKind::Sticker => Some((AttachmentDecision::Skipped, "sticker")),
        AttachmentKind::Audio | AttachmentKind::Video => {
            Some((AttachmentDecision::Skipped, "unsupported-kind"))
        }
        AttachmentKind::Image if !is_supported_image_mime(&candidate.mime) => {
            Some((AttachmentDecision::Rejected, "unsupported-image-mime"))
        }
        AttachmentKind::Image | AttachmentKind::File => None,
    }
}

/// MIME types the TS bridge accepts for image attachments.
fn is_supported_image_mime(mime: &str) -> bool {
    matches!(
        base_mime(mime).as_str(),
        "image/jpeg" | "image/png" | "image/webp" | "image/gif"
    )
}

/// Extension allow-list; anything else becomes `bin` (no path injection).
pub fn safe_extension_for_mime(mime: &str) -> &'static str {
    match base_mime(mime).as_str() {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "application/pdf" => "pdf",
        "application/zip" => "zip",
        "text/plain" => "txt",
        "text/markdown" => "md",
        "application/json" => "json",
        _ => "bin",
    }
}

fn base_mime(mime: &str) -> String {
    mime.split(';')
        .next()
        .unwrap_or(mime)
        .trim()
        .to_ascii_lowercase()
}

/// Downloader abstraction so the run flow can be tested offline.
///
/// Production uses [`SdkResourceDownloader`]; tests inject a fake. The method
/// takes the full descriptor so the implementation decides between `image_key`
/// (`type=image`) and `file_key` (`type=file`) exactly like the SDK.
#[async_trait]
pub trait ResourceDownloader: Send + Sync {
    async fn download(
        &self,
        resource: &ResourceDescriptor,
    ) -> Result<DownloadedResource, MediaError>;
}

#[derive(Debug, thiserror::Error)]
pub enum MediaError {
    #[error("{0} resources are not supported by the message resource download API")]
    Unsupported(&'static str),

    #[error("download failed: {0}")]
    Download(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl MediaError {
    fn unsupported(resource_type: ResourceType) -> Self {
        match resource_type {
            ResourceType::Folder => Self::Unsupported("folder"),
            ResourceType::Sticker => Self::Unsupported("sticker"),
            _ => Self::Unsupported("unknown"),
        }
    }
}

/// Downloader backed by `lark-channel`'s authenticated binary transport.
pub struct SdkResourceDownloader {
    downloader: lark_channel::MediaDownloader<lark_channel::lark_openapi::ReqwestOpenApiTransport>,
}

impl SdkResourceDownloader {
    pub fn new(channel: &larkline_lark::Channel) -> Self {
        Self {
            downloader: channel.media_downloader(),
        }
    }
}

#[async_trait]
impl ResourceDownloader for SdkResourceDownloader {
    async fn download(
        &self,
        resource: &ResourceDescriptor,
    ) -> Result<DownloadedResource, MediaError> {
        self.downloader
            .download(resource)
            .await
            .map_err(|error| match resource.resource_type {
                ResourceType::Folder | ResourceType::Sticker | ResourceType::Unknown => {
                    MediaError::unsupported(resource.resource_type)
                }
                _ => MediaError::Download(error.to_string()),
            })
    }
}

/// Bounded on-disk cache for inbound attachment bytes.
#[derive(Clone)]
pub struct MediaCache {
    root: PathBuf,
    downloader: Arc<dyn ResourceDownloader>,
}

impl std::fmt::Debug for MediaCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MediaCache")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl MediaCache {
    pub fn new(root: impl Into<PathBuf>, downloader: Arc<dyn ResourceDownloader>) -> Self {
        Self {
            root: root.into(),
            downloader,
        }
    }

    /// Cache rooted at the profile's `media/` directory.
    pub fn for_channel(paths: &AppPaths, channel: &larkline_lark::Channel) -> Self {
        Self::new(
            paths.media_dir.clone(),
            Arc::new(SdkResourceDownloader::new(channel)),
        )
    }

    /// Same cache with a different downloader (tests inject a fake).
    pub fn with_downloader(&self, downloader: Arc<dyn ResourceDownloader>) -> Self {
        Self {
            root: self.root.clone(),
            downloader,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve every resource on the batch, never failing the run.
    ///
    /// Download errors are logged and the resource is omitted; policy
    /// rejections stay in the result (with their reason) so the prompt can
    /// report them, but their bytes are removed from the cache unless another
    /// accepted attachment references the same content hash.
    pub async fn resolve(
        &self,
        resources: &[ResourceDescriptor],
        policy: &MediaPolicy,
    ) -> Vec<NormalizedAttachment> {
        if resources.is_empty() {
            return Vec::new();
        }
        if let Err(error) = tokio::fs::create_dir_all(&self.root).await {
            tracing::warn!(
                root = %self.root.display(),
                error = %error,
                "cannot create the media cache directory; skipping attachments"
            );
            return Vec::new();
        }

        let mut candidates = Vec::new();
        for resource in resources {
            match self.resolve_one(resource).await {
                Ok(Some(candidate)) => candidates.push(candidate),
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    message_id = %resource.message_id,
                    resource_type = ?resource.resource_type,
                    error = %error,
                    "attachment download failed; continuing without it"
                ),
            }
        }

        let normalized = normalize_attachments(&candidates, policy);
        self.remove_non_accepted(&normalized).await;
        let protect: HashSet<PathBuf> = normalized
            .iter()
            .filter(|attachment| attachment.is_accepted())
            .map(|attachment| attachment.path.clone())
            .collect();
        if let Err(error) = self.evict(policy, &protect).await {
            tracing::warn!(error = %error, "media cache eviction failed");
        }
        normalized
    }

    async fn resolve_one(
        &self,
        resource: &ResourceDescriptor,
    ) -> Result<Option<AttachmentCandidate>, MediaError> {
        let Some(kind) = AttachmentKind::from_resource_type(resource.resource_type) else {
            tracing::info!(
                message_id = %resource.message_id,
                resource_type = ?resource.resource_type,
                "skipping an attachment kind the message resource API does not support"
            );
            return Ok(None);
        };
        // Stickers cannot be downloaded and TS skips them before resolving.
        if kind == AttachmentKind::Sticker {
            tracing::info!(
                message_id = %resource.message_id,
                "skipping a sticker attachment"
            );
            return Ok(None);
        }

        let downloaded = self.downloader.download(resource).await?;
        let mime = downloaded
            .content_type
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| kind.default_mime().to_string());
        let hash = sha256_hex(&downloaded.bytes);
        let extension = safe_extension_for_mime(&mime);
        let abs_path = self.root.join(format!("{hash}.{extension}"));
        let size = downloaded.bytes.len() as u64;

        if tokio::fs::metadata(&abs_path).await.is_ok() {
            tracing::info!(path = %abs_path.display(), "media cache-hit");
        } else {
            self.write_atomic(&abs_path, &downloaded.bytes).await?;
        }

        let source_file_key = resource
            .file_key
            .clone()
            .or_else(|| resource.image_key.clone())
            .unwrap_or_default();
        tracing::info!(
            path = %abs_path.display(),
            size,
            "attachment downloaded"
        );
        Ok(Some(AttachmentCandidate {
            abs_path,
            kind,
            size,
            mime,
            hash,
            source_message_id: resource.message_id.clone(),
            source_file_key,
            original_name: resource.file_name.clone(),
        }))
    }

    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let tmp = self.root.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let result = self.write_and_rename(&tmp, path, bytes).await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        result
    }

    async fn write_and_rename(&self, tmp: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options.open(tmp).await?;
        file.write_all(bytes).await?;
        file.flush().await?;
        drop(file);
        match tokio::fs::rename(tmp, path).await {
            Ok(()) => Ok(()),
            Err(error) => {
                // A concurrent resolve of identical content won the race.
                if tokio::fs::metadata(path).await.is_ok() {
                    let _ = tokio::fs::remove_file(tmp).await;
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Delete the bytes of rejected/skipped attachments.
    ///
    /// A path still referenced by an accepted attachment is kept: content
    /// hashing means two messages with identical bytes share one cache file.
    async fn remove_non_accepted(&self, attachments: &[NormalizedAttachment]) {
        let accepted: HashSet<&Path> = attachments
            .iter()
            .filter(|attachment| attachment.is_accepted())
            .map(|attachment| attachment.path.as_path())
            .collect();
        for attachment in attachments
            .iter()
            .filter(|attachment| !attachment.is_accepted())
        {
            if accepted.contains(attachment.path.as_path()) {
                continue;
            }
            if let Err(error) = tokio::fs::remove_file(&attachment.path).await {
                tracing::debug!(
                    path = %attachment.path.display(),
                    error = %error,
                    "cannot remove a non-accepted attachment"
                );
            }
        }
    }

    /// Evict by TTL, then entry count, then total bytes (oldest first).
    ///
    /// Returns how many files were removed. Paths in `protect` (the current
    /// run's accepted attachments) are never removed.
    pub async fn evict(
        &self,
        policy: &MediaPolicy,
        protect: &HashSet<PathBuf>,
    ) -> std::io::Result<usize> {
        let mut files = list_cache_files(&self.root).await?;
        let mut removed: HashSet<PathBuf> = HashSet::new();
        let mut removed_count = 0usize;

        if !policy.cache_ttl.is_zero() {
            if let Some(cutoff) = SystemTime::now().checked_sub(policy.cache_ttl) {
                for file in &files {
                    if file.modified < cutoff
                        && !protect.contains(&file.path)
                        && remove_cache_file(&file.path).await
                    {
                        removed.insert(file.path.clone());
                        removed_count += 1;
                    }
                }
            }
        }

        files.sort_by_key(|file| file.modified);
        if policy.cache_max_files > 0 {
            let live = files
                .iter()
                .filter(|file| !removed.contains(&file.path))
                .count();
            let mut excess = live.saturating_sub(policy.cache_max_files);
            for file in &files {
                if excess == 0 {
                    break;
                }
                if removed.contains(&file.path) || protect.contains(&file.path) {
                    continue;
                }
                if remove_cache_file(&file.path).await {
                    removed.insert(file.path.clone());
                    removed_count += 1;
                    excess -= 1;
                }
            }
        }

        if policy.cache_max_bytes > 0 {
            let mut total: u64 = files
                .iter()
                .filter(|file| !removed.contains(&file.path))
                .map(|file| file.size)
                .sum();
            for file in &files {
                if total <= policy.cache_max_bytes {
                    break;
                }
                if removed.contains(&file.path) || protect.contains(&file.path) {
                    continue;
                }
                if remove_cache_file(&file.path).await {
                    removed.insert(file.path.clone());
                    removed_count += 1;
                    total = total.saturating_sub(file.size);
                }
            }
        }

        Ok(removed_count)
    }
}

struct CacheFile {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

async fn list_cache_files(root: &Path) -> std::io::Result<Vec<CacheFile>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(_) => break,
            };
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Ok(metadata) = entry.metadata().await else {
                continue;
            };
            files.push(CacheFile {
                path: entry.path(),
                size: metadata.len(),
                modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
    }
    Ok(files)
}

async fn remove_cache_file(path: &Path) -> bool {
    match tokio::fs::remove_file(path).await {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(
                path = %path.display(),
                error = %error,
                "cannot evict a cached attachment"
            );
            false
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Offline tests: fake downloader, no network.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    use super::*;

    struct FakeDownloader {
        files: StdMutex<HashMap<String, Result<Vec<u8>, String>>>,
        content_type: StdMutex<String>,
        calls: StdMutex<Vec<String>>,
    }

    impl FakeDownloader {
        fn new() -> Self {
            Self {
                files: StdMutex::new(HashMap::new()),
                content_type: StdMutex::new("image/png".to_string()),
                calls: StdMutex::new(Vec::new()),
            }
        }

        fn with(mut self, key: &str, bytes: &[u8]) -> Self {
            self.files
                .get_mut()
                .expect("files")
                .insert(key.to_string(), Ok(bytes.to_vec()));
            self
        }

        fn failing(mut self, key: &str) -> Self {
            self.files
                .get_mut()
                .expect("files")
                .insert(key.to_string(), Err("resource not found".to_string()));
            self
        }

        fn with_content_type(mut self, mime: &str) -> Self {
            *self.content_type.get_mut().expect("content type") = mime.to_string();
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }
    }

    #[async_trait]
    impl ResourceDownloader for FakeDownloader {
        async fn download(
            &self,
            resource: &ResourceDescriptor,
        ) -> Result<DownloadedResource, MediaError> {
            let key = resource
                .image_key
                .clone()
                .or_else(|| resource.file_key.clone())
                .unwrap_or_default();
            self.calls.lock().expect("calls").push(key.clone());
            let result = self
                .files
                .lock()
                .expect("files")
                .get(&key)
                .cloned()
                .unwrap_or_else(|| Err(format!("missing fixture for {key}")));
            result
                .map(|bytes| DownloadedResource {
                    bytes,
                    content_type: Some(self.content_type.lock().expect("content type").clone()),
                    content_disposition: None,
                })
                .map_err(MediaError::Download)
        }
    }

    fn image(message_id: &str, image_key: &str) -> ResourceDescriptor {
        ResourceDescriptor {
            message_id: message_id.to_string(),
            resource_type: ResourceType::Image,
            file_key: None,
            image_key: Some(image_key.to_string()),
            file_name: None,
            duration_ms: None,
        }
    }

    fn file(message_id: &str, file_key: &str, name: &str) -> ResourceDescriptor {
        ResourceDescriptor {
            message_id: message_id.to_string(),
            resource_type: ResourceType::File,
            file_key: Some(file_key.to_string()),
            image_key: None,
            file_name: Some(name.to_string()),
            duration_ms: None,
        }
    }

    fn sticker(message_id: &str, file_key: &str) -> ResourceDescriptor {
        ResourceDescriptor {
            message_id: message_id.to_string(),
            resource_type: ResourceType::Sticker,
            file_key: Some(file_key.to_string()),
            image_key: None,
            file_name: None,
            duration_ms: None,
        }
    }

    fn cache_with(downloader: FakeDownloader) -> (tempfile::TempDir, MediaCache) {
        let dir = tempfile::tempdir().expect("temp dir");
        let cache = MediaCache::new(dir.path().join("media"), Arc::new(downloader));
        (dir, cache)
    }

    fn candidate(kind: AttachmentKind, size: u64, mime: &str) -> AttachmentCandidate {
        AttachmentCandidate {
            abs_path: PathBuf::from(format!("/tmp/{size}.{mime}")),
            kind,
            size,
            mime: mime.to_string(),
            hash: format!("hash-{size}"),
            source_message_id: "om_1".to_string(),
            source_file_key: "file_1".to_string(),
            original_name: Some("../../escape.png".to_string()),
        }
    }

    #[test]
    fn defaults_match_the_typescript_caps() {
        let policy = MediaPolicy::default();
        assert_eq!(policy.max_count, 10);
        assert_eq!(policy.max_bytes, 100 * 1024 * 1024);
        assert_eq!(policy.max_file_bytes, 25 * 1024 * 1024);
        assert_eq!(policy.image_max_bytes, 25 * 1024 * 1024);
        assert_eq!(policy.cache_ttl, Duration::from_secs(24 * 60 * 60));
        assert_eq!(policy.cache_max_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn profile_attachments_block_overrides_the_defaults() {
        let mut profile = ProfileConfig::default();
        profile.extra.insert(
            "attachments".to_string(),
            serde_json::json!({
                "maxCount": 3,
                "maxBytes": 2048,
                "maxFileBytes": 1024,
                "imageMaxBytes": 512,
                "cacheTtlMs": 1000,
                "cacheMaxBytes": 4096,
                "cacheMaxFiles": 7
            }),
        );
        let policy = MediaPolicy::from_profile(&profile);
        assert_eq!(policy.max_count, 3);
        assert_eq!(policy.max_bytes, 2048);
        assert_eq!(policy.max_file_bytes, 1024);
        assert_eq!(policy.image_max_bytes, 512);
        assert_eq!(policy.cache_ttl, Duration::from_millis(1000));
        assert_eq!(policy.cache_max_bytes, 4096);
        assert_eq!(policy.cache_max_files, 7);

        // Invalid values are ignored one by one, like the TS numberOr.
        profile.extra.insert(
            "attachments".to_string(),
            serde_json::json!({"maxCount": "many", "maxBytes": 1}),
        );
        let policy = MediaPolicy::from_profile(&profile);
        assert_eq!(policy.max_count, 10);
        assert_eq!(policy.max_bytes, 1);
    }

    #[test]
    fn normalization_enforces_the_typescript_caps_in_order() {
        let policy = MediaPolicy {
            max_count: 2,
            max_bytes: 100,
            max_file_bytes: 80,
            image_max_bytes: 60,
            ..MediaPolicy::default()
        };
        let attachments = normalize_attachments(
            &[
                candidate(AttachmentKind::Image, 50, "image/png"),
                candidate(AttachmentKind::File, 50, "application/pdf"),
                candidate(AttachmentKind::File, 90, "application/pdf"),
                candidate(AttachmentKind::File, 1, "application/octet-stream"),
                candidate(AttachmentKind::Audio, 1, "audio/ogg"),
                candidate(AttachmentKind::Sticker, 1, "image/png"),
                candidate(AttachmentKind::Image, 1, "image/heic"),
            ],
            &policy,
        );

        assert_eq!(attachments[0].decision, AttachmentDecision::Accepted);
        assert_eq!(attachments[1].decision, AttachmentDecision::Accepted);
        // Count cap wins before the size caps are considered.
        assert_eq!(
            attachments[2].rejection_reason.as_deref(),
            Some("too-many-attachments")
        );
        assert_eq!(
            attachments[3].rejection_reason.as_deref(),
            Some("too-many-attachments")
        );
        // Early decisions do not consume the count budget.
        assert_eq!(attachments[4].decision, AttachmentDecision::Skipped);
        assert_eq!(
            attachments[4].rejection_reason.as_deref(),
            Some("unsupported-kind")
        );
        assert_eq!(attachments[5].decision, AttachmentDecision::Skipped);
        assert_eq!(attachments[5].rejection_reason.as_deref(), Some("sticker"));
        assert_eq!(attachments[6].decision, AttachmentDecision::Rejected);
        assert_eq!(
            attachments[6].rejection_reason.as_deref(),
            Some("unsupported-image-mime")
        );
        assert_eq!(
            attachments[0].requiredness,
            AttachmentRequiredness::Optional
        );

        // File/image size caps and the run byte cap are reported verbatim.
        let policy = MediaPolicy {
            max_count: 10,
            max_bytes: 100,
            max_file_bytes: 80,
            image_max_bytes: 60,
            ..MediaPolicy::default()
        };
        let attachments = normalize_attachments(
            &[
                candidate(AttachmentKind::File, 81, "application/pdf"),
                candidate(AttachmentKind::Image, 61, "image/png"),
                candidate(AttachmentKind::File, 50, "application/pdf"),
                candidate(AttachmentKind::File, 51, "application/pdf"),
            ],
            &policy,
        );
        assert_eq!(
            attachments[0].rejection_reason.as_deref(),
            Some("file-too-large")
        );
        assert_eq!(
            attachments[1].rejection_reason.as_deref(),
            Some("image-too-large")
        );
        assert_eq!(attachments[2].decision, AttachmentDecision::Accepted);
        assert_eq!(
            attachments[3].rejection_reason.as_deref(),
            Some("run-too-large")
        );
    }

    #[test]
    fn safe_extensions_come_from_an_allow_list() {
        assert_eq!(safe_extension_for_mime("image/png"), "png");
        assert_eq!(safe_extension_for_mime("IMAGE/JPEG; charset=binary"), "jpg");
        assert_eq!(safe_extension_for_mime("application/pdf"), "pdf");
        assert_eq!(safe_extension_for_mime("text/markdown"), "md");
        assert_eq!(
            safe_extension_for_mime("application/x-evil; name=../../x"),
            "bin"
        );
    }

    #[tokio::test]
    async fn cache_names_files_by_content_hash_and_reuses_them() {
        let downloader = FakeDownloader::new()
            .with("img_1", b"png-bytes")
            .with("img_2", b"png-bytes");
        let (_dir, cache) = cache_with(downloader);

        let first = cache
            .resolve(&[image("om_1", "img_1")], &MediaPolicy::default())
            .await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].decision, AttachmentDecision::Accepted);
        assert_eq!(first[0].mime, "image/png");
        assert_eq!(first[0].size, 9);

        let expected_hash = sha256_hex(b"png-bytes");
        let expected_path = cache.root().join(format!("{expected_hash}.png"));
        assert_eq!(first[0].path, expected_path);
        assert!(expected_path.exists(), "bytes must be on disk");

        // The original file name never reaches the path.
        assert!(!first[0].path.to_string_lossy().contains("escape"));

        // Identical content from another message reuses the same file.
        let second = cache
            .resolve(&[image("om_2", "img_2")], &MediaPolicy::default())
            .await;
        assert_eq!(second[0].path, expected_path);
        assert_eq!(second[0].source_message_id, "om_2");

        let entries: Vec<_> = std::fs::read_dir(cache.root())
            .expect("cache dir")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        assert_eq!(entries.len(), 1, "expected one cached file: {entries:?}");
        assert!(
            !entries
                .iter()
                .any(|name| name.to_string_lossy().starts_with(".tmp-")),
            "no temp files may survive an atomic write"
        );
    }

    #[tokio::test]
    async fn a_failed_download_does_not_fail_the_resolve() {
        let downloader = FakeDownloader::new()
            .with("img_ok", b"ok")
            .failing("img_bad");
        let (_dir, cache) = cache_with(downloader);

        let attachments = cache
            .resolve(
                &[image("om_1", "img_bad"), image("om_2", "img_ok")],
                &MediaPolicy::default(),
            )
            .await;

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].source_message_id, "om_2");
        assert!(attachments[0].is_accepted());
    }

    #[tokio::test]
    async fn stickers_are_skipped_before_any_download() {
        let dir = tempfile::tempdir().expect("temp dir");
        let downloader = Arc::new(FakeDownloader::new());
        let cache = MediaCache::new(dir.path().join("media"), downloader.clone());

        let attachments = cache
            .resolve(&[sticker("om_1", "sticker_1")], &MediaPolicy::default())
            .await;
        assert!(attachments.is_empty());
        assert!(
            downloader.calls().is_empty(),
            "a sticker must never reach the download API"
        );
    }

    #[tokio::test]
    async fn oversized_attachments_are_rejected_and_their_bytes_removed() {
        let downloader = FakeDownloader::new()
            .with("file_big", b"big-bytes")
            .with("file_small", b"ok");
        let (_dir, cache) = cache_with(downloader);
        let policy = MediaPolicy {
            max_file_bytes: 4,
            ..MediaPolicy::default()
        };

        let attachments = cache
            .resolve(
                &[
                    file("om_1", "file_big", "big.pdf"),
                    file("om_2", "file_small", "ok.pdf"),
                ],
                &policy,
            )
            .await;

        assert_eq!(attachments.len(), 2);
        assert_eq!(attachments[0].decision, AttachmentDecision::Rejected);
        assert_eq!(
            attachments[0].rejection_reason.as_deref(),
            Some("file-too-large")
        );
        assert!(!attachments[0].path.exists());
        assert!(attachments[1].is_accepted());
        assert!(attachments[1].path.exists());
    }

    #[tokio::test]
    async fn ttl_eviction_removes_only_unprotected_old_files() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("media");
        std::fs::create_dir_all(&root).expect("media dir");
        let old = root.join("old.png");
        let fresh = root.join("fresh.png");
        std::fs::write(&old, b"old").expect("write old");
        std::fs::write(&fresh, b"fresh").expect("write fresh");
        let old_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&old)
            .expect("open old");
        old_file
            .set_modified(SystemTime::now() - Duration::from_secs(48 * 60 * 60))
            .expect("age old file");

        let cache = MediaCache::new(&root, Arc::new(FakeDownloader::new()));
        let protect = HashSet::from([old.clone()]);
        let policy = MediaPolicy {
            cache_ttl: Duration::from_secs(60 * 60),
            ..MediaPolicy::default()
        };
        assert_eq!(cache.evict(&policy, &protect).await.expect("evict"), 0);
        assert!(old.exists(), "protected files survive TTL eviction");
        assert!(fresh.exists());

        let removed = cache.evict(&policy, &HashSet::new()).await.expect("evict");
        assert_eq!(removed, 1);
        assert!(!old.exists());
        assert!(fresh.exists());
    }

    #[tokio::test]
    async fn size_and_count_caps_evict_the_oldest_files() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("media");
        std::fs::create_dir_all(&root).expect("media dir");
        let now = SystemTime::now();
        for (name, size, age_secs) in [
            ("oldest.bin", 40u64, 30),
            ("middle.bin", 40, 20),
            ("newest.bin", 10, 10),
        ] {
            let path = root.join(name);
            std::fs::write(&path, vec![0u8; size as usize]).expect("write cache file");
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open cache file");
            file.set_modified(now - Duration::from_secs(age_secs))
                .expect("age cache file");
        }

        let cache = MediaCache::new(&root, Arc::new(FakeDownloader::new()));
        let policy = MediaPolicy {
            cache_ttl: Duration::ZERO,
            cache_max_bytes: 1000,
            cache_max_files: 2,
            ..MediaPolicy::default()
        };
        let removed = cache.evict(&policy, &HashSet::new()).await.expect("evict");
        assert_eq!(removed, 1, "count cap evicts exactly one file");
        assert!(!root.join("oldest.bin").exists());
        assert!(root.join("middle.bin").exists());
        assert!(root.join("newest.bin").exists());

        // Byte cap: 50 bytes total, keep only the newest 10.
        let removed = cache
            .evict(
                &MediaPolicy {
                    cache_max_bytes: 15,
                    cache_max_files: 10,
                    ..policy
                },
                &HashSet::new(),
            )
            .await
            .expect("evict");
        assert_eq!(removed, 1);
        assert!(!root.join("middle.bin").exists());
        assert!(root.join("newest.bin").exists());
    }

    #[tokio::test]
    async fn resolve_prompt_shape_matches_typescript() {
        let downloader = FakeDownloader::new()
            .with("file_1", b"pdf")
            .with_content_type("application/pdf");
        let (_dir, cache) = cache_with(downloader);
        let attachments = cache
            .resolve(
                &[file("om_9", "file_1", "report.pdf")],
                &MediaPolicy::default(),
            )
            .await;

        let prompt = to_prompt_attachments(&attachments);
        assert_eq!(prompt.len(), 1);
        let value = serde_json::to_value(&prompt[0]).expect("json");
        assert_eq!(value["kind"], "file");
        assert_eq!(value["mime"], "application/pdf");
        assert_eq!(value["sourceMessageId"], "om_9");
        assert_eq!(value["requiredness"], "optional");
        assert_eq!(value["decision"], "accepted");
        assert_eq!(value["size"], 3);
        assert!(value.get("rejectionReason").is_none());
        assert!(
            attachments[0]
                .path
                .extension()
                .is_some_and(|ext| ext == "pdf")
        );
        assert_eq!(attachments[0].original_name.as_deref(), Some("report.pdf"));
    }
}
