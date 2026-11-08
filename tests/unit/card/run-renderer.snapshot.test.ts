import { describe, expect, it } from 'vitest';
import { renderCard } from '../../../src/card/run-renderer.js';
import {
  initialState,
  markIdleTimeout,
  markInterrupted,
  reduce,
  type RunState,
} from '../../../src/card/run-state.js';
import { renderText } from '../../../src/card/text-renderer.js';
import type { AgentEvent } from '../../../src/agent/types.js';
import { normalizeCard } from '../../helpers/card-normalize.js';

describe('run card renderer snapshots', () => {
  it('renders initial running state', () => {
    expectCard(initialState).toMatchSnapshot();
  });

  it('renders active and completed thinking', () => {
    expectCard(stateFrom([{ type: 'thinking', delta: 'checking options' }])).toMatchSnapshot();
    expectCard(stateFrom([
      { type: 'thinking', delta: 'checking options' },
      { type: 'text', delta: 'final answer' },
      { type: 'done', terminationReason: 'normal' },
    ])).toMatchSnapshot();
  });

  it('renders tool running, done, and error states', () => {
    expectCard(stateFrom([
      { type: 'tool_use', id: 'tool-1', name: 'Bash', input: { command: 'pwd' } },
    ])).toMatchSnapshot();

    expectCard(stateFrom([
      { type: 'tool_use', id: 'tool-1', name: 'Bash', input: { command: 'pwd' } },
      { type: 'tool_result', id: 'tool-1', output: '/repo', isError: false },
      { type: 'done', terminationReason: 'normal' },
    ])).toMatchSnapshot();

    expectCard(stateFrom([
      { type: 'tool_use', id: 'tool-2', name: 'Read', input: { file_path: '/missing.ts' } },
      { type: 'tool_result', id: 'tool-2', output: 'ENOENT', isError: true },
      { type: 'done', terminationReason: 'normal' },
    ])).toMatchSnapshot();
  });

  it('collapses consecutive tools while preserving the latest running tool', () => {
    expectCard(stateFrom([
      { type: 'tool_use', id: 'tool-1', name: 'Bash', input: { command: 'pwd' } },
      { type: 'tool_result', id: 'tool-1', output: '/repo', isError: false },
      { type: 'tool_use', id: 'tool-2', name: 'Read', input: { file_path: '/repo/a.ts' } },
      { type: 'tool_result', id: 'tool-2', output: 'a', isError: false },
      { type: 'tool_use', id: 'tool-3', name: 'Edit', input: { file_path: '/repo/a.ts' } },
    ])).toMatchSnapshot();

    expectCard(stateFrom([
      { type: 'tool_use', id: 'tool-1', name: 'Bash', input: { command: 'pwd' } },
      { type: 'tool_result', id: 'tool-1', output: '/repo', isError: false },
      { type: 'tool_use', id: 'tool-2', name: 'Read', input: { file_path: '/repo/a.ts' } },
      { type: 'tool_result', id: 'tool-2', output: 'a', isError: false },
      { type: 'tool_use', id: 'tool-3', name: 'Edit', input: { file_path: '/repo/a.ts' } },
      { type: 'tool_result', id: 'tool-3', output: 'ok', isError: false },
      { type: 'done', terminationReason: 'normal' },
    ])).toMatchSnapshot();
  });

  it('renders done, error, interrupted, and idle-timeout terminal states', () => {
    expectCard(stateFrom([{ type: 'done', terminationReason: 'normal' }])).toMatchSnapshot();
    expectCard(stateFrom([{ type: 'error', message: 'process failed', terminationReason: 'failed' }])).toMatchSnapshot();
    expectCard(markInterrupted(stateFrom([{ type: 'text', delta: 'partial' }]))).toMatchSnapshot();
    expectCard(markIdleTimeout(stateFrom([{ type: 'text', delta: 'partial' }]), 15)).toMatchSnapshot();
  });

  it('renders markdown text mode without card-only controls', () => {
    const state = stateFrom([
      { type: 'thinking', delta: 'hidden reasoning' },
      { type: 'text', delta: 'Answer' },
      { type: 'tool_use', id: 'tool-1', name: 'Bash', input: { command: 'pwd' } },
      { type: 'tool_result', id: 'tool-1', output: '/repo', isError: false },
      { type: 'text', delta: 'Done' },
    ]);

    expect(renderText(state)).toMatchSnapshot();
    expect(renderText(markInterrupted(state))).toMatchSnapshot();
    expect(renderText(markIdleTimeout(state, 10))).toMatchSnapshot();
    expect(renderText(stateFrom([{ type: 'error', message: 'process failed', terminationReason: 'failed' }]))).toMatchSnapshot();
  });

  it('injects signed bridge callback values for managed run controls', () => {
    const card = renderCard(initialState, {
      signCallback: (action) => `token-for-${action}`,
    }) as {
      body?: { elements?: Array<{ tag?: string; behaviors?: Array<{ value?: Record<string, unknown> }> }> };
    };
    const button = card.body?.elements?.find((element) => element.tag === 'button');

    expect(button?.behaviors?.[0]?.value).toEqual({
      cmd: 'stop',
      __bridge_cb: true,
      bridge_token: 'token-for-stop',
    });
  });

  it('keeps local paths in user-visible cards and text fallbacks', () => {
    const sensitivePath = '/Users/example/private/customer/repo/secret.txt';
    const state = stateFrom([
      { type: 'text', delta: `I read ${sensitivePath}` },
      { type: 'tool_use', id: 'tool-1', name: 'Read', input: { file_path: sensitivePath } },
      { type: 'tool_result', id: 'tool-1', output: `content from ${sensitivePath}`, isError: false },
      { type: 'done', terminationReason: 'normal' },
    ]);

    const card = JSON.stringify(renderCard(state));
    const text = renderText(state);
    expect(card).toContain(sensitivePath);
    expect(text).toContain(sensitivePath);
  });
});

describe('renderText reasoning option', () => {
  it('hides reasoning by default', () => {
    const state = stateFrom([
      { type: 'thinking', delta: 'hidden reasoning' },
      { type: 'text', delta: 'Answer' },
    ]);

    expect(renderText(state)).not.toContain('hidden reasoning');
    expect(renderText(state, {})).toBe(renderText(state));
    expect(renderText(state, { reasoning: false })).toBe(renderText(state));
  });

  it('prepends active reasoning as a markdown blockquote when enabled', () => {
    const state = stateFrom([{ type: 'thinking', delta: 'line 1\n\nline 2' }]);

    expect(renderText(state, { reasoning: true })).toBe(
      '🧠 **思考中**\n> line 1\n>\n> line 2\n\n_🧠 正在思考…_',
    );
  });

  it('uses the completed title and puts reasoning before normal blocks', () => {
    const state = stateFrom([
      { type: 'thinking', delta: 'done thinking' },
      { type: 'text', delta: 'Answer' },
      { type: 'done', terminationReason: 'normal' },
    ]);
    const text = renderText(state, { reasoning: true });

    expect(text).toBe('🧠 **思考过程**\n> done thinking\n\nAnswer');
    expect(text.indexOf('🧠 **思考过程**')).toBeLessThan(text.indexOf('Answer'));
  });

  it('truncates reasoning to reasoningMaxChars and appends a note', () => {
    const state = stateFrom([{ type: 'thinking', delta: '0123456789EXTRA' }]);

    const text = renderText(state, { reasoning: true, reasoningMaxChars: 10 });

    expect(text).toContain('> 0123456789\n> …（思考内容已截断）');
    expect(text).not.toContain('EXTRA');
  });

  it('defaults the reasoning limit to 3000 characters', () => {
    const head = 'a'.repeat(3000);
    const state = stateFrom([{ type: 'thinking', delta: `${head}TAIL` }]);

    const text = renderText(state, { reasoning: true });

    expect(text).toContain(`> ${head}`);
    expect(text).not.toContain('TAIL');
    expect(text).toContain('> …（思考内容已截断）');
  });

  it('masks emails and neutralizes HTML in reasoning', () => {
    const state = stateFrom([{ type: 'thinking', delta: 'mail me@example.com <b>hi</b>' }]);

    const text = renderText(state, { reasoning: true });

    expect(text).toContain('me[at]example.com');
    expect(text).not.toContain('me@example.com');
    expect(text).not.toContain('<b>');
    expect(text).toContain('&lt;b>hi&lt;/b>');
  });
});

function stateFrom(events: AgentEvent[]): RunState {
  return events.reduce((state, event) => reduce(state, event), initialState);
}

function expectCard(state: RunState) {
  return expect(normalizeCard(renderCard(state)));
}
