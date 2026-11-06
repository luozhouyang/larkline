import { describe, expect, it } from 'vitest';
import { buildPlist } from '../../../src/daemon/launchd';
import {
  daemonStderrPath,
  daemonStdoutPath,
  launchAgentLabel,
  serviceProfileId,
  systemdUnitName,
  windowsTaskName,
} from '../../../src/daemon/paths';
import { buildLauncherCmd } from '../../../src/daemon/schtasks';
import { buildUnit } from '../../../src/daemon/systemd';

describe('profile-scoped daemon paths and arguments', () => {
  it('sanitizes service ids and gives each profile distinct service names and logs', () => {
    // ASCII-safe names pass through unchanged (stable existing labels).
    expect(serviceProfileId('codex_dev')).toBe('codex_dev');
    expect(serviceProfileId('codex-dev')).toBe('codex-dev');
    // Non-ASCII / label-unsafe names get a deterministic ASCII-safe id.
    expect(serviceProfileId('codex dev')).toMatch(/^codex-dev-[0-9a-f]{8}$/);
    expect(serviceProfileId('助手')).toMatch(/^profile-[0-9a-f]{8}$/);
    expect(serviceProfileId('助手')).toBe(serviceProfileId('助手'));
    expect(() => serviceProfileId('.')).toThrow(/invalid profile name/i);
    expect(launchAgentLabel('codex-dev')).toContain('codex-dev');
    expect(systemdUnitName('claude')).not.toBe(systemdUnitName('codex-dev'));
    expect(windowsTaskName('claude')).not.toBe(windowsTaskName('codex-dev'));
    expect(daemonStdoutPath('claude')).not.toBe(daemonStdoutPath('codex-dev'));
    expect(daemonStderrPath('codex-dev').replace(/\\/g, '/')).toContain(
      '/profiles/codex-dev/logs/daemon/',
    );
  });

  it('classic service pins `run --profile <profile>` and LARKLINE_HOME', () => {
    const inputs = {
      nodePath: '/usr/local/bin/node',
      bridgeEntryPath: '/repo/bin/larkline.mjs',
      envPath: '/usr/local/bin:/usr/bin',
      profile: 'codex-dev',
      runArgs: ['run', '--profile', 'codex-dev'],
      channelHome: '/tmp/larkline-home',
    };

    // Classic per-profile service pins --profile so the daemon always runs THIS
    // profile regardless of active-profile changes.
    expect(buildPlist(inputs)).toContain('<string>run</string>');
    expect(buildPlist(inputs)).toContain('<string>--profile</string>');
    expect(buildPlist(inputs)).toContain('<string>codex-dev</string>');
    expect(buildPlist(inputs)).toContain('<key>LARKLINE_HOME</key>\n        <string>/tmp/larkline-home</string>');
    expect(buildUnit(inputs)).toContain('run --profile codex-dev');
    expect(buildUnit(inputs)).toContain('Environment="LARKLINE_HOME=/tmp/larkline-home"');
    expect(buildLauncherCmd(inputs)).toContain('run --profile codex-dev');
    expect(buildLauncherCmd(inputs)).toContain('set "LARKLINE_HOME=/tmp/larkline-home"');
  });

  it('supervisor service runs `run --web-ui` with no --profile', () => {
    const inputs = {
      nodePath: '/usr/local/bin/node',
      bridgeEntryPath: '/repo/bin/larkline.mjs',
      envPath: '/usr/local/bin:/usr/bin',
      profile: 'supervisor',
      runArgs: ['run', '--web-ui'],
      channelHome: '/tmp/larkline-home',
    };

    expect(buildPlist(inputs)).toContain('<string>--web-ui</string>');
    expect(buildPlist(inputs)).not.toContain('--profile');
    expect(buildUnit(inputs)).toContain('run --web-ui');
    expect(buildUnit(inputs)).not.toContain('--profile');
    expect(buildLauncherCmd(inputs)).toContain('run --web-ui');
    expect(buildLauncherCmd(inputs)).not.toContain('--profile');
  });
});
