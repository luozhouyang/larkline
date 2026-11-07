import { describe, expect, it, vi } from 'vitest';
import type { LarkChannel } from '@larksuite/channel';
import {
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
