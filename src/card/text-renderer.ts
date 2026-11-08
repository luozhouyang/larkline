import { maskEmails } from './mask-email';
import type { Block, RunState, ToolEntry } from './run-state';
import { toolHeaderText } from './tool-render';

const DEFAULT_REASONING_MAX_CHARS = 3000;

export interface TextRenderOptions {
  /** Include aggregated thinking/reasoning. Default false preserves current behavior. */
  reasoning?: boolean;
  /** Max characters of reasoning to include. Default 3000. */
  reasoningMaxChars?: number;
}

/**
 * Render `RunState` as plain markdown text — used in `messageReply: 'text'`
 * mode where we stream a markdown message instead of a card.
 *
 * Differences vs `renderCard`:
 *   - No collapsible panels, no buttons (markdown messages have neither)
 *   - Tool calls collapse to a single short line each (no body)
 *   - Reasoning is omitted by default; pass `{ reasoning: true }` to prepend
 *     it as a markdown blockquote
 *   - Footer is appended inline at the bottom while running
 */
export function renderText(state: RunState, options: TextRenderOptions = {}): string {
  const parts: string[] = [];

  if (options.reasoning === true && state.reasoning.content) {
    parts.push(reasoningSection(state.reasoning.content, state.reasoning.active, options.reasoningMaxChars));
  }

  for (const block of state.blocks) {
    const piece = renderBlock(block);
    if (piece) parts.push(piece);
  }

  if (state.terminal === 'interrupted') {
    parts.push('_⏹ 已被中断_');
  } else if (state.terminal === 'idle_timeout') {
    const mins = state.idleTimeoutMinutes ?? 0;
    parts.push(`_⏱ ${mins} 分钟无响应,已自动终止_`);
  } else if (state.terminal === 'error' && state.errorMsg) {
    parts.push(`⚠️ agent 失败:${state.errorMsg}`);
  } else if (state.terminal === 'running' && state.footer) {
    parts.push(footerLine(state.footer));
  }

  // Strip raw emails so the Feishu tenant audit doesn't reject the message
  // (see mask-email.ts). Never removes content, so emptiness checks upstream
  // still behave.
  return maskEmails(parts.join('\n\n'));
}

/**
 * Reasoning as title + markdown blockquote. Every line is prefixed with `> `
 * (blank lines become a bare `>`), so the section stays pure markdown — no raw
 * HTML wrappers. `<` is escaped because reasoning is model-authored and may
 * contain HTML tags, which must not become live markup in the message.
 */
function reasoningSection(content: string, active: boolean, maxChars?: number): string {
  const limit =
    typeof maxChars === 'number' && Number.isFinite(maxChars)
      ? Math.max(0, Math.floor(maxChars))
      : DEFAULT_REASONING_MAX_CHARS;
  const body = content.trim();
  const truncated = body.length > limit;
  const clipped = (truncated ? body.slice(0, limit) : body).trimEnd();
  const lines = [active ? '🧠 **思考中**' : '🧠 **思考过程**'];

  if (clipped) {
    lines.push(
      clipped
        .replace(/</g, '&lt;')
        .split('\n')
        .map((line) => (line.length > 0 ? `> ${line}` : '>'))
        .join('\n'),
    );
  }
  if (truncated) lines.push('> …（思考内容已截断）');

  return lines.join('\n');
}

function renderBlock(block: Block): string {
  if (block.kind === 'text') {
    return block.content.trim();
  }
  return toolLine(block.tool);
}

/**
 * One-line summary for a tool call:
 *   `> ⏳ **Bash** — git status`
 *   `> ✅ **Read** — ~/code/foo.ts`
 * Reuses `toolHeaderText` so the format matches the card mode header.
 */
function toolLine(tool: ToolEntry): string {
  return `> ${toolHeaderText(tool)}`;
}

function footerLine(status: 'thinking' | 'tool_running' | 'streaming'): string {
  if (status === 'thinking') return '_🧠 正在思考…_';
  if (status === 'tool_running') return '_🧰 正在调用工具…_';
  return '_✍️ 正在输出…_';
}
