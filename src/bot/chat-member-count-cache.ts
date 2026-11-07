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
