/**
 * Read a bridge-owned environment variable under its `LARKLINE_*` name.
 *
 * The project was renamed from `lark-channel-bridge` to `larkline`; existing
 * daemons, shell profiles, and scripts may still export `LARK_CHANNEL_*`.
 * Prefer the new name and fall back to the legacy one so upgrades keep
 * working. New deployments should only use the new name.
 */
export function readBridgeEnv(name: string): string | undefined {
  const current = process.env[`LARKLINE_${name}`];
  if (current) return current;
  return process.env[`LARK_CHANNEL_${name}`] || undefined;
}
