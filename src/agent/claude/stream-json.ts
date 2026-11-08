import type { AgentEvent } from '../types';

interface ContentBlock {
  type: string;
  text?: string;
  thinking?: string;
  id?: string;
  name?: string;
  input?: unknown;
  tool_use_id?: string;
  content?: unknown;
  is_error?: boolean;
}

interface StreamEvent {
  type?: string;
  message?: { id?: string };
  delta?: {
    type?: string;
    thinking?: string;
    text?: string;
  };
  content_block?: { type?: string };
}

interface ClaudeRawEvent {
  type?: string;
  subtype?: string;
  session_id?: string;
  cwd?: string;
  model?: string;
  message?: { id?: string; content?: ContentBlock[] };
  event?: StreamEvent;
  usage?: {
    input_tokens?: number;
    output_tokens?: number;
    cache_read_input_tokens?: number;
  };
  total_cost_usd?: number;
}

/**
 * Stateful translator for Claude Code's stream-json output.
 *
 * With `--include-partial-messages`, thinking arrives twice: once as
 * `stream_event/content_block_delta/thinking_delta` chunks and again inside the
 * completed `assistant` message. We stream the deltas and remember which
 * message ids already produced partial thinking, then skip the duplicate
 * thinking blocks on the corresponding assistant event. Text and tool blocks
 * still come from the completed assistant event (partial text deltas are
 * ignored), so the existing final-answer path is unchanged.
 */
export class ClaudeStreamTranslator {
  private currentMessageId: string | undefined;
  private readonly partialThinkingMessages = new Set<string>();

  *translate(raw: unknown): Generator<AgentEvent> {
    if (!raw || typeof raw !== 'object') return;
    const evt = raw as ClaudeRawEvent;

    if (evt.type === 'stream_event') {
      yield* this.translateStreamEvent(evt.event);
      return;
    }

    if (evt.type === 'system' && evt.subtype === 'init') {
      yield {
        type: 'system',
        sessionId: evt.session_id,
        cwd: evt.cwd,
        model: evt.model,
      };
      return;
    }

    if (evt.type === 'assistant' && evt.message?.content) {
      const messageId = evt.message.id;
      for (const block of evt.message.content) {
        if (block.type === 'text' && typeof block.text === 'string' && block.text) {
          yield { type: 'text', delta: block.text };
        } else if (block.type === 'thinking' && typeof block.thinking === 'string' && block.thinking) {
          // The partial stream already emitted this message's thinking.
          if (messageId && this.partialThinkingMessages.has(messageId)) continue;
          yield { type: 'thinking', delta: block.thinking };
        } else if (block.type === 'tool_use' && block.id && block.name) {
          yield { type: 'tool_use', id: block.id, name: block.name, input: block.input };
        }
      }
      return;
    }

    if (evt.type === 'user' && evt.message?.content) {
      for (const block of evt.message.content) {
        if (block.type === 'tool_result' && block.tool_use_id) {
          const output =
            typeof block.content === 'string' ? block.content : JSON.stringify(block.content);
          yield {
            type: 'tool_result',
            id: block.tool_use_id,
            output,
            isError: block.is_error === true,
          };
        }
      }
      return;
    }

    if (evt.type === 'result') {
      if (evt.usage) {
        yield {
          type: 'usage',
          inputTokens: evt.usage.input_tokens,
          outputTokens: evt.usage.output_tokens,
          cachedInputTokens: evt.usage.cache_read_input_tokens,
          costUsd: evt.total_cost_usd,
        };
      }
      yield { type: 'done', sessionId: evt.session_id, terminationReason: 'normal' };
    }
  }

  private *translateStreamEvent(event: StreamEvent | undefined): Generator<AgentEvent> {
    if (!event) return;
    if (event.type === 'message_start') {
      this.currentMessageId = event.message?.id;
      return;
    }
    if (event.type === 'content_block_delta' && event.delta?.type === 'thinking_delta') {
      const text = event.delta.thinking;
      if (!text) return;
      if (this.currentMessageId) this.partialThinkingMessages.add(this.currentMessageId);
      yield { type: 'thinking', delta: text };
    }
  }
}

/** Stateless helper for translating a single raw event. The adapter uses
 * {@link ClaudeStreamTranslator} so partial thinking can be deduplicated. */
export function* translateEvent(raw: unknown): Generator<AgentEvent> {
  yield* new ClaudeStreamTranslator().translate(raw);
}
