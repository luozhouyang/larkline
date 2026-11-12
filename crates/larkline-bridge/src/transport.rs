//! Injectable outbound transport for the bridge.
//!
//! Production wraps one [`Channel`]; tests inject a fake so the whole run flow
//! (running card, streaming updates, terminal card, final answer) can be
//! exercised end-to-end without a network. The trait deliberately covers only
//! the message/card operations the run flow performs:
//!
//! * text / markdown / card sends and threaded replies ([`reply`](crate::reply));
//! * CardKit entity creation and streaming card handles ([`BridgeStream`]);
//! * message-card updates and direct messages (meeting replies).
//!
//! The rest of the Lark surface (tenant-token REST calls for comments, COT,
//! meetings and owner lookup) stays on [`RestClient`], which is already
//! injectable at its own call sites.

use async_trait::async_trait;
use lark_channel::message::{MessageId, Recipient};
use lark_channel::{Card, CardId};
use larkline_lark::{Channel, LarkResult, RestClient, RestResult, StreamingCard};

use larkline_lark::reactions::{add_reaction, remove_reaction};

/// A live CardKit streaming card.
///
/// Production wraps [`StreamingCard`]; tests return a fake that records every
/// frame. The object stays alive for the length of one run.
#[async_trait]
pub trait BridgeStream: Send {
    /// The message that references the streaming entity.
    fn message_id(&self) -> &str;

    /// True while the card may still be updated.
    fn is_live(&self) -> bool;

    /// Replace the streamed element content.
    async fn set_content(&mut self, content: &str) -> LarkResult<bool>;

    /// Replace the whole card document.
    async fn set_card(&mut self, card: &Card) -> LarkResult<bool>;

    /// Write the final content and close the card.
    async fn finish(&mut self, content: &str) -> LarkResult<bool>;
}

/// Where a streaming card's reference message is delivered.
#[derive(Debug, Clone, Copy)]
pub enum StreamTarget<'a> {
    /// A top-level message in a chat.
    Chat(&'a str),
    /// A threaded reply to an existing message.
    Thread(&'a MessageId),
}

/// Outbound message/card operations used by the bridge run flow.
///
/// Implemented by [`Channel`] in production. A fake implementation only has
/// to return a [`MessageId`] / [`CardId`] and record what it was asked to do.
#[async_trait]
pub trait BridgeTransport: Send + Sync {
    async fn send_text(&self, chat_id: &str, text: String) -> LarkResult<MessageId>;

    /// Direct-message text to one open id (meeting replies).
    async fn send_dm_text(&self, open_id: &str, text: String) -> LarkResult<MessageId>;

    async fn send_markdown(&self, chat_id: &str, markdown: String) -> LarkResult<MessageId>;

    async fn send_card(&self, chat_id: &str, card: &Card) -> LarkResult<MessageId>;

    async fn reply_text(&self, parent: &MessageId, text: String) -> LarkResult<MessageId>;

    async fn reply_markdown(&self, parent: &MessageId, markdown: String) -> LarkResult<MessageId>;

    async fn reply_card(&self, parent: &MessageId, card: &Card) -> LarkResult<MessageId>;

    /// Send a message that references an existing CardKit entity.
    async fn send_card_reference(&self, chat_id: &str, card_id: &CardId) -> LarkResult<MessageId>;

    /// Reply with a reference to an existing CardKit entity.
    async fn reply_card_reference(
        &self,
        parent: &MessageId,
        card_id: &CardId,
    ) -> LarkResult<MessageId>;

    /// Replace the card attached to an existing message.
    async fn update_card_message(&self, message_id: &str, card: &Card) -> LarkResult<()>;

    /// Create a CardKit entity (the streaming path updates it by id).
    async fn create_card_entity(&self, card: &Card) -> LarkResult<CardId>;

    /// Create a streaming card and send the message that references it.
    async fn start_stream(
        &self,
        target: StreamTarget<'_>,
        card: Card,
        element_id: &str,
    ) -> LarkResult<Box<dyn BridgeStream>>;
}

#[async_trait]
impl BridgeStream for StreamingCard {
    fn message_id(&self) -> &str {
        &self.message_id().0
    }

    fn is_live(&self) -> bool {
        StreamingCard::is_live(self)
    }

    async fn set_content(&mut self, content: &str) -> LarkResult<bool> {
        StreamingCard::set_content(self, content).await
    }

    async fn set_card(&mut self, card: &Card) -> LarkResult<bool> {
        StreamingCard::set_card(self, card).await
    }

    async fn finish(&mut self, content: &str) -> LarkResult<bool> {
        StreamingCard::finish(self, content).await
    }
}

#[async_trait]
impl BridgeTransport for Channel {
    async fn send_text(&self, chat_id: &str, text: String) -> LarkResult<MessageId> {
        Channel::send_text(self, chat_id, text).await
    }

    async fn send_dm_text(&self, open_id: &str, text: String) -> LarkResult<MessageId> {
        self.sender()
            .text_message(Recipient::User(open_id.to_owned()), text)
            .send()
            .await
    }

    async fn send_markdown(&self, chat_id: &str, markdown: String) -> LarkResult<MessageId> {
        self.sender()
            .markdown_message(Recipient::Chat(chat_id.to_owned()), markdown)
            .send()
            .await
    }

    async fn send_card(&self, chat_id: &str, card: &Card) -> LarkResult<MessageId> {
        Channel::send_card(self, chat_id, card).await
    }

    async fn reply_text(&self, parent: &MessageId, text: String) -> LarkResult<MessageId> {
        self.sender()
            .text_reply(parent.clone(), text)
            .reply_in_thread(true)
            .send()
            .await
    }

    async fn reply_markdown(&self, parent: &MessageId, markdown: String) -> LarkResult<MessageId> {
        self.sender()
            .markdown_reply(parent.clone(), markdown)
            .reply_in_thread(true)
            .send()
            .await
    }

    async fn reply_card(&self, parent: &MessageId, card: &Card) -> LarkResult<MessageId> {
        self.sender()
            .card_reply(parent.clone(), card.clone())
            .reply_in_thread(true)
            .send()
            .await
    }

    async fn send_card_reference(&self, chat_id: &str, card_id: &CardId) -> LarkResult<MessageId> {
        self.sender()
            .card_reference_message(Recipient::Chat(chat_id.to_owned()), card_id.clone())
            .send()
            .await
    }

    async fn reply_card_reference(
        &self,
        parent: &MessageId,
        card_id: &CardId,
    ) -> LarkResult<MessageId> {
        self.sender()
            .card_reference_reply(parent.clone(), card_id.clone())
            .reply_in_thread(true)
            .send()
            .await
    }

    async fn update_card_message(&self, message_id: &str, card: &Card) -> LarkResult<()> {
        Channel::update_card_message(self, message_id, card).await
    }

    async fn create_card_entity(&self, card: &Card) -> LarkResult<CardId> {
        Channel::create_card_entity(self, card).await
    }

    async fn start_stream(
        &self,
        target: StreamTarget<'_>,
        card: Card,
        element_id: &str,
    ) -> LarkResult<Box<dyn BridgeStream>> {
        let stream = match target {
            StreamTarget::Chat(chat_id) => {
                StreamingCard::start(self, chat_id, card, element_id).await?
            }
            StreamTarget::Thread(parent) => {
                StreamingCard::start_reply(self, parent.clone(), card, element_id).await?
            }
        };
        Ok(Box::new(stream))
    }
}

/// Reaction calls made while a run is live.
///
/// Production uses [`RestClient`]'s `im/v1/messageReaction` endpoints. The
/// fake in the run-flow tests is a no-op, which keeps those tests offline and
/// deterministic.
#[async_trait]
pub trait ReactionTransport: Send + Sync {
    async fn add(&self, message_id: &str, emoji_type: &str) -> RestResult<String>;
    async fn remove(&self, message_id: &str, reaction_id: &str) -> RestResult<()>;
}

#[async_trait]
impl ReactionTransport for RestClient {
    async fn add(&self, message_id: &str, emoji_type: &str) -> RestResult<String> {
        add_reaction(self, message_id, emoji_type).await
    }

    async fn remove(&self, message_id: &str, reaction_id: &str) -> RestResult<()> {
        remove_reaction(self, message_id, reaction_id).await
    }
}
