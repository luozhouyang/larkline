import type { LarkChannel } from '@larksuite/channel';
import { log } from '../core/logger';

/** How long a chat's human-member count is trusted before re-probing. */
export const DEFAULT_CHAT_MEMBER_COUNT_TTL_MS = 60_000;

interface CachedMemberCount {
  memberCount?: number;
  expiresAt: number;
}

/**
 * In-memory cache for `channel.getChatInfo().memberCount` — Feishu's
 * `user_count`, which counts human members and excludes bots (`bot_count` is
 * a separate field).
 *
 * Why: the bridge relaxes the group @-mention requirement for chats that only
 * contain one human (the requester) plus the bot, so they behave like DMs.
 * The count is not on the message event, and probing `im.v1.chat.get` on every
 * message would be wasteful. A short TTL keeps the relaxation dynamic: when a
 * second person joins, the next message after the TTL requires @ again.
 *
 * On lookup failure or a missing count we fall back to `false` (keep the
 * normal @ requirement) and cache the failure briefly so a flaky API can't
 * create a request storm.
 */
export class ChatMemberCountCache {
  private readonly cache = new Map<string, CachedMemberCount>();
  private readonly ttlMs: number;
  private readonly now: () => number;

  constructor(opts: { ttlMs?: number; now?: () => number } = {}) {
    this.ttlMs = opts.ttlMs ?? DEFAULT_CHAT_MEMBER_COUNT_TTL_MS;
    this.now = opts.now ?? Date.now;
  }

  /** True when the chat has exactly one human member (bot members excluded). */
  async isSoloHumanChat(channel: LarkChannel, chatId: string): Promise<boolean> {
    const now = this.now();
    const hit = this.cache.get(chatId);
    if (hit && hit.expiresAt > now) return hit.memberCount === 1;

    try {
      const info = await channel.getChatInfo(chatId);
      const memberCount = info.memberCount;
      this.cache.set(chatId, { memberCount, expiresAt: now + this.ttlMs });
      log.info('chat', 'member-count-resolved', { chatId, memberCount: memberCount ?? null });
      // Leave room for doubt when Feishu omits the count — keep requiring @.
      return memberCount === 1;
    } catch (err) {
      this.cache.set(chatId, {
        memberCount: undefined,
        expiresAt: now + Math.min(this.ttlMs, 30_000),
      });
      log.warn('chat', 'member-count-resolve-failed', {
        chatId,
        err: err instanceof Error ? err.message : String(err),
      });
      return false;
    }
  }

  invalidate(chatId: string): void {
    this.cache.delete(chatId);
  }
}

const MEMBER_CHANGE_EVENTS = [
  'im.chat.member.user.added_v1',
  'im.chat.member.user.deleted_v1',
  'im.chat.member.user.withdrawn_v1',
] as const;

/**
 * Drop the cached human count as soon as Feishu tells us membership changed,
 * so a freshly added/removed member takes effect on the next message instead
 * of waiting for {@link DEFAULT_CHAT_MEMBER_COUNT_TTL_MS}.
 *
 * Delivery requires the app to subscribe to the `im.chat.member.user.*` events
 * in the developer console; when it doesn't, the TTL still converges. The
 * subscription is best-effort because older `@larksuite/channel` builds may
 * not expose `onRawEvent`.
 */
export function bindChatMemberCountInvalidation(
  channel: LarkChannel,
  cache: ChatMemberCountCache,
): () => void {
  const subscribe = channel.onRawEvent?.bind(channel);
  if (!subscribe) return () => {};

  const unsubscribers = MEMBER_CHANGE_EVENTS.map((eventType) =>
    subscribe(eventType, (payload) => {
      const chatId = memberChangeChatId(payload);
      if (!chatId) return;
      cache.invalidate(chatId);
      log.info('chat', 'member-count-invalidated', { chatId, eventType });
    }),
  );
  return () => {
    for (const unsubscribe of unsubscribers) unsubscribe();
  };
}

/**
 * `onRawEvent` normally hands us the unwrapped event body (`chat_id` at the
 * top level), but tolerate the `{ event: { chat_id } }` envelope too.
 */
function memberChangeChatId(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') return undefined;
  const record = payload as { chat_id?: unknown; event?: unknown };
  if (typeof record.chat_id === 'string') return record.chat_id;
  if (record.event && typeof record.event === 'object') {
    const nested = record.event as { chat_id?: unknown };
    if (typeof nested.chat_id === 'string') return nested.chat_id;
  }
  return undefined;
}
