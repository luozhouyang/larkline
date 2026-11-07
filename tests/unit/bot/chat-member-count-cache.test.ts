import { describe, expect, it, vi } from 'vitest';
import type { LarkChannel } from '@larksuite/channel';
import {
  bindChatMemberCountInvalidation,
  ChatMemberCountCache,
  DEFAULT_CHAT_MEMBER_COUNT_TTL_MS,
} from '../../../src/bot/chat-member-count-cache.js';

function fakeChannel(getChatInfo: () => Promise<{ memberCount?: number }>): {
  channel: LarkChannel;
  calls: ReturnType<typeof vi.fn>;
} {
  const calls = vi.fn(getChatInfo);
  return {
    channel: { getChatInfo: calls } as unknown as LarkChannel,
    calls,
  };
}

describe('ChatMemberCountCache', () => {
  it('treats a one-human chat as solo and caches the probe', async () => {
    const { channel, calls } = fakeChannel(async () => ({ memberCount: 1 }));
    const cache = new ChatMemberCountCache();

    await expect(cache.isSoloHumanChat(channel, 'oc_one')).resolves.toBe(true);
    await expect(cache.isSoloHumanChat(channel, 'oc_one')).resolves.toBe(true);
    expect(calls).toHaveBeenCalledTimes(1);
  });

  it('keeps the @ requirement once a second human joins', async () => {
    let memberCount = 1;
    let now = 0;
    const { channel, calls } = fakeChannel(async () => ({ memberCount }));
    const cache = new ChatMemberCountCache({ ttlMs: 100, now: () => now });

    await expect(cache.isSoloHumanChat(channel, 'oc_group')).resolves.toBe(true);

    memberCount = 2;
    now = 50;
    // Still inside the TTL: cached answer wins.
    await expect(cache.isSoloHumanChat(channel, 'oc_group')).resolves.toBe(true);
    expect(calls).toHaveBeenCalledTimes(1);

    now = 100;
    await expect(cache.isSoloHumanChat(channel, 'oc_group')).resolves.toBe(false);
    expect(calls).toHaveBeenCalledTimes(2);
  });

  it('falls back to requiring @ when the count is missing or the API fails', async () => {
    const missing = fakeChannel(async () => ({}));
    const missingCache = new ChatMemberCountCache();
    await expect(missingCache.isSoloHumanChat(missing.channel, 'oc_missing')).resolves.toBe(false);

    const failing = fakeChannel(async () => {
      throw new Error('permission denied');
    });
    const failingCache = new ChatMemberCountCache();
    await expect(failingCache.isSoloHumanChat(failing.channel, 'oc_fail')).resolves.toBe(false);
    // The failure is cached briefly, so an immediately following message does
    // not hammer the API again.
    await expect(failingCache.isSoloHumanChat(failing.channel, 'oc_fail')).resolves.toBe(false);
    expect(failing.calls).toHaveBeenCalledTimes(1);
  });

  it('invalidates the cached count when a member-change event arrives', async () => {
    let memberCount = 1;
    const handlers = new Map<string, Set<(payload: unknown) => void>>();
    const getChatInfo = vi.fn(async () => ({ memberCount }));
    const channel = {
      getChatInfo,
      onRawEvent(eventType: string, handler: (payload: unknown) => void) {
        let set = handlers.get(eventType);
        if (!set) {
          set = new Set();
          handlers.set(eventType, set);
        }
        set.add(handler);
        return () => set?.delete(handler);
      },
    } as unknown as LarkChannel;
    const cache = new ChatMemberCountCache();
    const stop = bindChatMemberCountInvalidation(channel, cache);

    await expect(cache.isSoloHumanChat(channel, 'oc_member_change')).resolves.toBe(true);

    // A second human joins: the event drops the cached count immediately, so
    // the very next message re-probes instead of waiting for the TTL.
    memberCount = 2;
    for (const handler of handlers.get('im.chat.member.user.added_v1') ?? []) {
      handler({ chat_id: 'oc_member_change' });
    }
    await expect(cache.isSoloHumanChat(channel, 'oc_member_change')).resolves.toBe(false);
    expect(getChatInfo).toHaveBeenCalledTimes(2);

    stop();
    expect(handlers.get('im.chat.member.user.added_v1')?.size).toBe(0);
  });

  it('accepts the nested event envelope shape', async () => {
    let memberCount = 1;
    const handlers = new Map<string, Set<(payload: unknown) => void>>();
    const channel = {
      getChatInfo: vi.fn(async () => ({ memberCount })),
      onRawEvent(eventType: string, handler: (payload: unknown) => void) {
        let set = handlers.get(eventType);
        if (!set) {
          set = new Set();
          handlers.set(eventType, set);
        }
        set.add(handler);
        return () => set?.delete(handler);
      },
    } as unknown as LarkChannel;
    const cache = new ChatMemberCountCache();
    bindChatMemberCountInvalidation(channel, cache);

    await cache.isSoloHumanChat(channel, 'oc_nested');
    memberCount = 2;
    for (const handler of handlers.get('im.chat.member.user.added_v1') ?? []) {
      handler({ event: { chat_id: 'oc_nested' } });
    }

    await expect(cache.isSoloHumanChat(channel, 'oc_nested')).resolves.toBe(false);
  });

  it('re-probes after the default TTL', async () => {
    let now = 0;
    const { channel, calls } = fakeChannel(async () => ({ memberCount: 1 }));
    const cache = new ChatMemberCountCache({ now: () => now });

    await cache.isSoloHumanChat(channel, 'oc_ttl');
    now += DEFAULT_CHAT_MEMBER_COUNT_TTL_MS;
    await cache.isSoloHumanChat(channel, 'oc_ttl');

    expect(calls).toHaveBeenCalledTimes(2);
  });
});
