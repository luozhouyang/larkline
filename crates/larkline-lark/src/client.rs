//! Thin wrapper around the `lark-channel` SDK.
//!
//! Everything that the SDK already models goes through it (messages, CardKit,
//! media, the event consumer). The bridge only wraps it so the rest of the
//! codebase depends on one small surface instead of the whole crate.

use lark_channel::lark_openapi::{OpenApiClient, ReqwestOpenApiTransport};
use lark_channel::message::{MessageContent, MessageId, MessageSender, Recipient};
use lark_channel::{Card, ChannelConfig, Domain};

use crate::rest::RestClient;

pub type LarkResult<T> = lark_channel::Result<T>;

/// One authenticated app connection.
#[derive(Clone)]
pub struct Channel {
    config: ChannelConfig,
    openapi: OpenApiClient<ReqwestOpenApiTransport>,
    sender: MessageSender<ReqwestOpenApiTransport>,
    rest: RestClient,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("app_id", &self.config.app_id)
            .finish_non_exhaustive()
    }
}

impl Channel {
    pub fn new(
        app_id: impl Into<String>,
        app_secret: impl Into<String>,
        tenant: larkline_core::TenantBrand,
    ) -> Self {
        let domain = match tenant {
            larkline_core::TenantBrand::Feishu => Domain::Feishu,
            larkline_core::TenantBrand::Lark => Domain::Lark,
        };
        let mut config = ChannelConfig::new(app_id, app_secret);
        config.domain = domain;
        let openapi = OpenApiClient::new(config.clone(), ReqwestOpenApiTransport::new());
        let sender = MessageSender::new(openapi.clone());
        let rest = RestClient::new(tenant, &config.app_id, &config.app_secret);
        Self {
            config,
            openapi,
            sender,
            rest,
        }
    }

    pub fn app_id(&self) -> &str {
        &self.config.app_id
    }

    pub fn openapi(&self) -> &OpenApiClient<ReqwestOpenApiTransport> {
        &self.openapi
    }

    pub fn sender(&self) -> &MessageSender<ReqwestOpenApiTransport> {
        &self.sender
    }

    pub fn rest(&self) -> &RestClient {
        &self.rest
    }

    pub async fn send_text(&self, chat_id: &str, text: impl Into<String>) -> LarkResult<MessageId> {
        self.sender
            .text_message(Recipient::Chat(chat_id.to_owned()), text)
            .send()
            .await
    }

    pub async fn send_card(&self, chat_id: &str, card: &Card) -> LarkResult<MessageId> {
        self.sender
            .card_message(Recipient::Chat(chat_id.to_owned()), card.clone())
            .send()
            .await
    }

    /// Replace the card attached to an existing message.
    pub async fn update_card_message(&self, message_id: &str, card: &Card) -> LarkResult<()> {
        let id = MessageId(message_id.to_owned());
        self.openapi.update_message_card(&id, card).await
    }

    /// Create a CardKit entity (the streaming path updates it by id).
    pub async fn create_card_entity(&self, card: &Card) -> LarkResult<lark_channel::CardId> {
        self.openapi.create_card_entity(card).await
    }

    /// Low-level send, for post/raw card payloads the typed helpers do not cover.
    pub async fn send_content(
        &self,
        chat_id: &str,
        content: MessageContent,
    ) -> LarkResult<MessageId> {
        self.sender
            .message(Recipient::Chat(chat_id.to_owned()), content)
            .send()
            .await
    }
}

/// Build the SDK domain value for a tenant.
pub fn domain_for(tenant: larkline_core::TenantBrand) -> Domain {
    match tenant {
        larkline_core::TenantBrand::Feishu => Domain::Feishu,
        larkline_core::TenantBrand::Lark => Domain::Lark,
    }
}
