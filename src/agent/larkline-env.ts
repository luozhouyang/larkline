import { join } from 'node:path';

export interface LarklineEnvContext {
  profile?: string;
  rootDir?: string;
  configPath?: string;
  larkCliConfigDir?: string;
  larkCliSourceConfigFile?: string;
}

export function buildLarklineEnv(context?: LarklineEnvContext): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    LARKLINE: '1',
    // Legacy handshake kept for the external `lark-cli` binary, which still
    // looks for the pre-rename variable. Bridge-owned variables below are also
    // mirrored so an older lark-cli build can keep resolving the same profile.
    LARK_CHANNEL: '1',
  };
  const profile = nonEmpty(context?.profile);
  if (profile) {
    env.LARKLINE_PROFILE = profile;
    env.LARK_CHANNEL_PROFILE = profile;
  }

  const rootDir = nonEmpty(context?.rootDir);
  if (rootDir) {
    env.LARKLINE_HOME = rootDir;
    env.LARK_CHANNEL_HOME = rootDir;
  }

  const configPath =
    nonEmpty(context?.larkCliSourceConfigFile) ??
    nonEmpty(context?.configPath) ??
    (rootDir ? join(rootDir, 'config.json') : undefined);
  if (configPath) {
    env.LARKLINE_CONFIG = configPath;
    env.LARK_CHANNEL_CONFIG = configPath;
  }

  const larkCliConfigDir = nonEmpty(context?.larkCliConfigDir);
  if (larkCliConfigDir) env.LARKSUITE_CLI_CONFIG_DIR = larkCliConfigDir;

  return env;
}

function nonEmpty(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? value : undefined;
}
