import type { NormalizedMessage } from '@larksuite/channel';
import { realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { AppPreferences } from '../../../src/config/schema.js';
import { createDefaultProfileConfig } from '../../../src/config/profile-schema.js';
import { SessionStore } from '../../../src/session/store.js';
import { WorkspaceStore } from '../../../src/workspace/store.js';
import { FakeAgentAdapter } from '../../helpers/fake-agent.js';
import { createTmpProfile, type TmpProfile } from '../../helpers/tmp-profile.js';

const sdkMock = vi.hoisted(() => ({
  channel: undefined as FakeLarkChannel | undefined,
  createLarkChannel: vi.fn(() => {
    if (!sdkMock.channel) throw new Error('fake channel not configured');
    return sdkMock.channel;
  }),
}));

vi.mock('@larksuite/channel', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@larksuite/channel')>();
  return {
    ...actual,
    createLarkChannel: sdkMock.createLarkChannel,
  };
});

import { startChannel } from '../../../src/bot/channel.js';

interface MessageHandlerMap {
  message?: (msg: NormalizedMessage) => Promise<void> | void;
}

interface FakeLarkChannel {
  botIdentity: { openId: string; name: string };
  rawClient: {
    request: ReturnType<typeof vi.fn>;
    im: {
      v1: {
        message: { get: ReturnType<typeof vi.fn> };
        messageReaction: {
          create: ReturnType<typeof vi.fn>;
          delete: ReturnType<typeof vi.fn>;
        };
      };
    };
    application: {
      v6: {
        application: { get: ReturnType<typeof vi.fn> };
      };
    };
  };
  on(handlers: MessageHandlerMap): void;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  getChatMode(chatId: string): Promise<'p2p'>;
  getConnectionStatus(): { state: 'connected'; reconnectAttempts: number };
  send(chatId: string, content: unknown, options?: unknown): Promise<void>;
  stream(chatId: string, input: unknown, options?: unknown): Promise<void>;
  readonly handlers: MessageHandlerMap;
  readonly markdownContents: string[][];
  readonly cardUpdates: unknown[][];
}

const cleanups: Array<() => Promise<void>> = [];

afterEach(async () => {
  sdkMock.channel = undefined;
  sdkMock.createLarkChannel.mockClear();
  await Promise.all(cleanups.splice(0).map((cleanup) => cleanup()));
});

describe('reasoning display in IM replies', () => {
  it('inlines reasoning in markdown mode when reasoningDisplay=inline', async () => {
    const h = await createHarness({ messageReply: 'markdown', reasoningDisplay: 'inline' });
    await startTestBridge(h);

    await h.channel.handlers.message?.(message('om_inline', 'hello'));
    await waitFor(() =>
      (lastContent(h.channel.markdownContents) ?? '').includes('final answer'),
    );

    const content = lastContent(h.channel.markdownContents) ?? '';
    expect(content).toContain('internal thought');
    expect(content).toContain('final answer');
  });

  it('keeps the legacy markdown layout when reasoningDisplay=auto', async () => {
    const h = await createHarness({ messageReply: 'markdown', reasoningDisplay: 'auto' });
    await startTestBridge(h);

    await h.channel.handlers.message?.(message('om_auto', 'hello'));
    await waitFor(() =>
      (lastContent(h.channel.markdownContents) ?? '').includes('final answer'),
    );

    const content = lastContent(h.channel.markdownContents) ?? '';
    expect(content).not.toContain('internal thought');
    expect(content).toContain('final answer');
  });

  it('hides the card thinking panel when reasoningDisplay=off', async () => {
    const h = await createHarness({ messageReply: 'card', reasoningDisplay: 'off' });
    await startTestBridge(h);

    await h.channel.handlers.message?.(message('om_card_off', 'hello'));
    await waitFor(() =>
      JSON.stringify(lastContent(h.channel.cardUpdates) ?? '').includes('final answer'),
    );

    const card = JSON.stringify(lastContent(h.channel.cardUpdates));
    expect(card).not.toContain('internal thought');
    expect(card).toContain('final answer');
  });
});

async function createHarness(preferences: AppPreferences): Promise<{
  tmp: TmpProfile;
  channel: FakeLarkChannel;
  agent: FakeAgentAdapter;
  sessions: SessionStore;
  workspaces: WorkspaceStore;
  profileConfig: ReturnType<typeof createDefaultProfileConfig>;
  controls: ReturnType<typeof createControls>;
}> {
  const tmp = await createTmpProfile('reasoning-display-');
  const workspace = await realpath(tmp.workspace);
  const profileConfig = createDefaultProfileConfig({
    agentKind: 'claude',
    accounts: { app: { id: 'cli_test', secret: 'secret', tenant: 'feishu' } },
    access: { allowedUsers: ['ou_user'] },
    preferences,
  });
  profileConfig.workspaces.default = workspace;
  const sessions = new SessionStore(join(tmp.profile, 'sessions.json'));
  const workspaces = new WorkspaceStore(join(tmp.profile, 'workspaces.json'));
  const agent = new FakeAgentAdapter({
    events: [
      { type: 'thinking', delta: 'internal thought' },
      { type: 'text', delta: 'final answer' },
      { type: 'done', terminationReason: 'normal' },
    ],
  });
  const channel = createFakeLarkChannel();
  sdkMock.channel = channel;
  const controls = createControls(profileConfig);
  cleanups.push(async () => {
    await Promise.all([sessions.flush(), workspaces.flush()]);
    await tmp.cleanup();
  });
  return { tmp, channel, agent, sessions, workspaces, profileConfig, controls };
}

async function startTestBridge(h: {
  profileConfig: ReturnType<typeof createDefaultProfileConfig>;
  agent: FakeAgentAdapter;
  sessions: SessionStore;
  workspaces: WorkspaceStore;
  controls: ReturnType<typeof createControls>;
}): Promise<void> {
  const bridge = await startChannel({
    cfg: h.profileConfig,
    agent: h.agent,
    sessions: h.sessions,
    workspaces: h.workspaces,
    controls: h.controls,
  });
  cleanups.push(() => bridge.disconnect());
}

function createFakeLarkChannel(): FakeLarkChannel {
  const handlers: MessageHandlerMap = {};
  const markdownContents: string[][] = [];
  const cardUpdates: unknown[][] = [];
  return {
    handlers,
    markdownContents,
    cardUpdates,
    botIdentity: { openId: 'ou_bot', name: 'Bridge' },
    rawClient: {
      request: vi.fn(async () => ({ data: { items: [] } })),
      im: {
        v1: {
          message: { get: vi.fn(async () => ({ data: { items: [] } })) },
          messageReaction: {
            create: vi.fn(async () => ({ data: { reaction_id: 'reaction_1' } })),
            delete: vi.fn(async () => ({})),
          },
        },
      },
      application: {
        v6: {
          application: {
            get: vi.fn(async () => ({ data: { app: { owner: { owner_id: 'ou_owner' } } } })),
          },
        },
      },
    },
    on(nextHandlers) {
      Object.assign(handlers, nextHandlers);
    },
    async connect() {},
    async disconnect() {},
    async getChatMode() {
      return 'p2p';
    },
    getConnectionStatus() {
      return { state: 'connected', reconnectAttempts: 0 };
    },
    async send() {},
    async stream(_chatId, input) {
      if (isMarkdownStreamInput(input)) {
        const contents: string[] = [];
        markdownContents.push(contents);
        await input.markdown({
          setContent: async (markdown: string) => {
            contents.push(markdown);
          },
        });
      } else if (isCardStreamInput(input)) {
        const updates: unknown[] = [];
        cardUpdates.push(updates);
        await input.card.producer({
          update: async (card: unknown) => {
            updates.push(card);
          },
        });
      }
    },
  };
}

function createControls(profileConfig: ReturnType<typeof createDefaultProfileConfig>) {
  return {
    profile: 'test',
    profileConfig,
    ownerRefreshState: 'unknown' as const,
    async refreshOwner() {},
    async restart() {},
    async exit() {},
    configPath: '/tmp/config.json',
    cfg: profileConfig,
    processId: 'proc_test',
  };
}

function message(messageId: string, content: string): NormalizedMessage {
  return {
    messageId,
    chatId: 'oc_dm',
    chatType: 'p2p',
    senderId: 'ou_user',
    senderName: 'User',
    content,
    rawContentType: 'text',
    resources: [],
    mentions: [],
    mentionAll: false,
    mentionedBot: false,
    createTime: 1760000001000,
  } as unknown as NormalizedMessage;
}

async function waitFor(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error('timed out waiting for async work');
}

function lastContent<T>(lists: T[][]): T | undefined {
  return lists.at(-1)?.at(-1);
}

interface MarkdownStreamInput {
  markdown(ctrl: { setContent(markdown: string): Promise<void> }): Promise<void> | void;
}

function isMarkdownStreamInput(input: unknown): input is MarkdownStreamInput {
  return Boolean(input && typeof input === 'object' && 'markdown' in input);
}

interface CardStreamInput {
  card: {
    producer(ctrl: { update(card: unknown): Promise<void> }): Promise<void> | void;
  };
}

function isCardStreamInput(input: unknown): input is CardStreamInput {
  return Boolean(input && typeof input === 'object' && 'card' in input);
}
