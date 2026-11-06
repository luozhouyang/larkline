#!/usr/bin/env node
import { existsSync, renameSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

// The project was renamed from lark-channel-bridge to larkline. Move the old
// state directory before the CLI imports path helpers, so a normal upgrade
// keeps profiles, sessions, and secrets without any manual step. An explicit
// LARKLINE_HOME / LARK_CHANNEL_HOME always wins.
if (!process.env.LARKLINE_HOME && !process.env.LARK_CHANNEL_HOME) {
  const current = join(homedir(), '.larkline');
  const legacy = join(homedir(), '.lark-channel');
  if (!existsSync(current) && existsSync(legacy)) {
    for (const [from, to] of [
      [legacy, current],
      [`${legacy}-workspaces`, `${current}-workspaces`],
    ]) {
      try {
        if (existsSync(from) && !existsSync(to)) renameSync(from, to);
      } catch {
        // Keep going: resolveAppPaths still accepts the legacy directory.
      }
    }
  }
}

await import('../dist/cli.js');
