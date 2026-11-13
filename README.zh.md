# larkline

larkline 是一个自包含的单个静态二进制文件，用来把飞书 / Lark 消息和本机 Claude Code 或 Codex CLI 打通。**不依赖 Node.js，不需要 npm；安装和运行时都只需要这一个二进制。** 启动 bridge，扫码绑定 PersonalAgent 应用，就可以在飞书里和本机编程助手对话。

larkline fork 自 [zarazhangrui/lark-coding-agent-bridge](https://github.com/zarazhangrui/lark-coding-agent-bridge)，由 `lark-channel-bridge` 更名而来。Rust 实现沿用 TypeScript 版本的 `~/.larkline` 状态目录、配置结构和加密 keystore，老用户不需要重新扫码或重新录入 App Secret。详见 [Rust 移植说明](./docs/RUST-PORT.md)。

[English README](./README.md) · [Rust 移植说明](./docs/RUST-PORT.md) · [产品说明](https://larkcommunity.feishu.cn/docx/OaRIdFIRFoLM3xxTmKwcetHqn5e)

## 主要功能

- 把飞书 / Lark 消息转发给本机 Claude Code 或 Codex CLI。可以私聊 bot，也可以在群里 `@bot`。
- **流式卡片**：agent 工作期间，答案和工具摘要持续更新在同一张飞书卡片上；运行中的卡片带停止按钮。超过约 8.5 分钟后会自动接到一张新卡片继续展示，而不是静默停在旧卡片。
- **会话延续**：每个聊天或话题线程恢复自己的 agent 会话；只有会话创建时使用的工作目录一致，才会恢复。
- **排队与消息合并**：短时间连续发送的消息会作为一轮处理；运行中收到的消息排队到下一轮，并写入磁盘，重启后自动恢复；`/stop` 会中断当前任务并丢弃排队消息。
- **全局并发上限**：同一时间最多运行 `preferences.maxConcurrentRuns` 个 agent 任务（默认 10），超出的消息继续排队。
- **按聊天/话题切换工作目录**：用 `/cd` 切换当前聊天或话题的工作目录，用 `/pwd` 查看。
- **云文档评论**：在支持的文档评论（`doc`、`docx`、`sheet` 或 `file`）里 `@bot`，bridge 会在同一个评论线程里回复。评论任务可以由管理员用 `/stop comment:<hash>` 中断。应用需要订阅 `drive.notice.comment_add_v1` 事件。
- **会议智能体（可选）**：让 bot 入会，在会中或通过私聊回答问题，并基于实时字幕生成纪要。会议任务可以由管理员用 `/stop meeting:<id>` 中断。
- **授权卡片**：当 agent 使用非 skip 的权限模式时，工具调用会变成带「允许 / 拒绝」按钮的卡片。只有发起本次任务的用户可以操作；10 分钟未处理会自动拒绝。见[权限与授权](#权限与授权)。
- **长任务保护**：飞书会在流式卡片创建约 10 分钟后停止接受更新。larkline 在约 8.5 分钟时创建一张新卡片继续展示进度，最终答案落在最新卡片上，避免长任务静默丢失结果。
- **系统服务**：可以前台调试，也可以安装按 profile 隔离的 launchd、systemd 或 Task Scheduler 服务。
- **多 profile**：同一台机器上可以维护多套 PersonalAgent 应用、agent、会话和工作目录。
- **加密 App Secret**：App Secret 存在本地加密 keystore 中，不会明文写进 `config.json`。

## 前置条件

- macOS、Linux 或 Windows。larkline 本身不需要 Node.js、npm 或任何语言运行时。
- 本机安装并登录至少一个编程 agent：
  - Claude Code：`claude`，安装说明：https://docs.anthropic.com/en/docs/claude-code/quickstart
  - Codex CLI：`codex`，安装说明：https://developers.openai.com/codex/cli
- 一个飞书 / Lark **PersonalAgent** 应用。`larkline onboard` 可以通过扫码流程创建并绑定。
- 运行 bridge 的机器能访问飞书 / Lark 网络。

agent CLI 是独立工具，仍然遵循它自己的安装要求（例如通过 Node.js 安装的 Claude Code 仍需要 Node.js）。larkline 只负责启动本机已有的二进制。

## 安装

先把安装脚本下载到本地，再执行：

```bash
curl -fsSL https://raw.githubusercontent.com/luozhouyang/larkline/main/install.sh -o /tmp/larkline-install.sh
sh /tmp/larkline-install.sh
```

故意分成两步：脚本先落到文件里，你可以在执行前检查内容，而不是把网络上的脚本直接通过管道交给 shell。

默认安装到 `~/.local/bin`，不需要 `sudo`。如果该目录不在 `PATH` 中，安装脚本会打印对应的 `export` 命令。

### 固定版本

```bash
sh /tmp/larkline-install.sh --version <version>
```

版本号前带 `v` 也可以。需要可复现安装或回滚时使用。

### 指定安装目录

```bash
LARKLINE_INSTALL_DIR="$HOME/bin" sh /tmp/larkline-install.sh
# 或
sh /tmp/larkline-install.sh --dir "$HOME/bin"
```

安装脚本先把新二进制写到目标目录旁边，再 rename 到位。正在运行的 daemon 会继续持有旧 inode 并完成手头工作；下一次启动自动使用新二进制。

### 校验和与平台产物

安装脚本会下载对应产物，并在发布提供 `SHA256SUMS` 时校验；如果某个版本没有校验和文件，会打印警告，而不是假装校验通过。

| 平台 | 发布产物 | 说明 |
|---|---|---|
| macOS（Apple Silicon + Intel） | `larkline-darwin-universal.zip` | Universal 二进制；配置好 Apple secrets 后由发布流水线签名并 notarize（目前仍待配置） |
| Linux x86_64 | `larkline-linux-x86_64.tar.gz` | 静态链接（musl），不依赖 glibc |
| Linux aarch64 | `larkline-linux-aarch64.tar.gz` | 静态链接（musl），在 Ubuntu 24.04 ARM 上构建 |
| Windows x86_64 | `larkline-windows-x86_64.zip` | 未签名；首次运行可能触发 SmartScreen 警告 |

Windows 上请从 release zip 解压 `larkline.exe` 并放进 `PATH`。安装脚本是 POSIX `sh` 脚本，Windows 上直接用 zip 最省事。

### 升级

```bash
larkline upgrade --check   # 只检查最新版本
larkline upgrade           # 打印对应的安装命令
```

两种形式都会请求 GitHub 的 releases 地址，因此需要能访问 GitHub。`--target-version <v>` 也会先做这次查询，再打印指定版本的安装命令，并不是离线模式。

`larkline upgrade` 不会在 daemon 运行时偷偷替换二进制。它只报告可用版本和安装命令，由你决定什么时候执行并重启服务。Windows 上用 release zip 替换 `larkline.exe`。

## 快速开始

### 1. 创建 profile

最省事的方式是通过扫码设备流创建 PersonalAgent 应用：

```bash
larkline onboard
```

终端会渲染二维码。用飞书 / Lark 扫码并在 App 内确认，profile 会写入 `~/.larkline/config.json`。默认 profile 名是 `bot`（之后是 `bot-2`、`bot-3`……）；可以用 `--name` 指定名字，`--agent codex` 选择 Codex，`--tenant lark` 选择 Lark 国际版。

如果已经有 PersonalAgent 应用，可以直接创建 profile：

```bash
larkline profile create claude --app-id cli_xxx
```

命令会提示输入 App Secret，输入时不回显。Codex 加 `--agent codex`，Lark 国际版加 `--tenant lark`；脚本化场景可以用 `--app-secret` 非交互传入。`--no-use` 表示创建后不切换为当前 profile。

不需要提前指定项目目录。bridge 会创建一个由 profile 托管的默认工作目录；之后在飞书 / Lark 里用 `/cd <绝对路径>` 切到实际项目。

### 2. 检查配置

```bash
larkline doctor
```

`doctor` 会检查状态目录、配置文件、profile、当前 profile、app id、keystore 解密、agent 二进制，并把 `lark-cli` 作为警告项检查。必要检查失败时退出码非 0。

### 3. 前台运行

```bash
larkline run
```

`run` 会打印 profile、app、tenant、agent 和工作目录，然后连接飞书 / Lark 长连接。可以私聊 bot，或在群里 `@bot`。按 `Ctrl-C` 停止。

用 `larkline run --dry-run` 可以在不连接的情况下校验配置、凭据和 agent 二进制。profile 没有配置 `workspaces.default` 时，`--cwd` 可以指定本次运行的工作目录。

### 4. 作为系统服务运行

先停掉前台进程，再安装并启动按 profile 隔离的 daemon：

```bash
larkline start
larkline status
```

`start` 会在需要时安装或更新系统服务定义，停掉已在运行的实例，然后启动新实例。服务定义会记录二进制路径、profile 和当前 shell 的 `PATH`，所以退出登录后 `claude`、`codex` 仍然可被发现。不要同时用 `larkline run` 和 daemon 连接同一个应用。

服务命令按 profile 注册：

```bash
larkline start [--profile <name>]
larkline stop [--profile <name>]        # 停止并关闭开机自启
larkline restart [--profile <name>]
larkline status [--profile <name>]
larkline unregister [--profile <name>]  # 删除系统服务注册
```

平台映射：

- **macOS**：launchd 用户代理 `ai.larkline.bot.<profile>`
- **Linux**：systemd 用户单元 `larkline.bot.<profile>.service`
- **Windows**：Task Scheduler 任务 `Larkline.Bot.<profile>`，通过生成的 `.cmd` 启动

daemon 的 stdout / stderr 日志在 `~/.larkline/profiles/<profile>/logs/daemon/`。

## 命令速查

| 命令 | 作用 |
|---|---|
| `larkline run [--cwd <path>] [--dry-run]` | 前台运行 bridge。`--dry-run` 只校验并打印将要运行的内容，不建立连接。 |
| `larkline doctor` | 检查配置、凭据、keystore、profile 和 agent 二进制。必要检查失败时退出码非 0。 |
| `larkline onboard [--name <name>] [--agent claude\|codex] [--tenant feishu\|lark] [--no-use]` | 扫码创建 profile（应用注册设备流）。 |
| `larkline profile list\|create\|use\|remove\|show` | 管理 profile。`create` 必须传 `--app-id`，还支持 `--app-secret`、`--agent`、`--tenant` 和 `--no-use`；`remove` 支持 `--yes` 和 `--purge-secret`；`show` 以 JSON 打印一个 profile。 |
| `larkline secrets get\|set\|list\|remove` | 管理加密 keystore。`get` 在 stdin/stdout 上使用 exec-provider JSON 协议；`list` 只打印 id，不打印值。 |
| `larkline upgrade [--check] [--target-version <v>]` | 报告最新版本并打印安装命令；不会自动替换二进制；需要网络访问。 |
| `larkline start` | 安装（如有需要）并启动系统服务。 |
| `larkline stop` | 停止 daemon 并关闭开机自启。 |
| `larkline restart` | 重启已安装的 daemon。 |
| `larkline status` | 显示服务定义、是否安装、是否运行、pid 和上次退出码。 |
| `larkline unregister` | 删除系统服务注册，保留本地状态。 |
| `larkline ps` | 列出共享进程注册表中记录的 bridge 进程，并标记已失效的条目。 |
| `larkline kill <id\|#>` | 按短 id 或从 1 开始的列表序号停止已注册的 bridge 进程。 |

全局选项：`--home <path>`（默认 `$LARKLINE_HOME` 或 `~/.larkline`）、`--profile <name>`（默认 `$LARKLINE_PROFILE` 或当前 profile）、`-v` / `-vv` 提高日志级别。

`ps` 和 `kill` 读取 `~/.larkline/registry/processes.json`。`larkline run` 启动时会注册自己的进程，正常退出和 ctrl-c/SIGTERM 时移除条目，因此 Rust 和 TypeScript 进程共用同一份列表。Rust 条目没有 `botName`（`lark-channel` 0.6 没有读取 bot 身份的接口）；`larkline ps` 显示 profile、agent、pid 和启动时间。

## 飞书内命令

| 命令 | 作用 |
|---|---|
| `/new`, `/reset` | 清空当前聊天/话题的会话。 |
| `/cd <绝对路径>` | 切换当前聊天/话题的工作目录。路径必须存在且为绝对路径，会被 canonicalize。 |
| `/pwd` | 显示当前生效的工作目录。 |
| `/status` | 显示 profile、agent、app id、tenant、cwd、回复模式、会话是否可恢复和正在运行的 scope。 |
| `/config` | 显示当前回复偏好和并发上限。 |
| `/config set messageReply <card\|markdown\|text>` | 设置回复模式（仅管理员）。 |
| `/config set showToolCalls <true\|false>` | 显示或隐藏工具摘要（仅管理员）。 |
| `/config set maxConcurrentRuns <1-50>` | 设置全局 agent 并发上限（仅管理员）。 |
| `/meeting` | 显示会议智能体状态。需要 `meeting.enabled`。 |
| `/meeting join <9 位会议号>` | 按 9 位会议号入会。 |
| `/meeting leave [会议号]` | 离开指定会议。 |
| `/meeting notes [会议号]` | 为指定会议生成纪要。 |
| `/meeting ask <问题>` | 带上会议字幕上下文提问。 |
| `/meeting transcript [会议号]` | 查看 agent 实际拿到的字幕上下文。 |
| `/meeting stop [会议号]` | 中断该会议当前的 agent 任务。 |
| `/stop` | 中断当前聊天/话题中的任务，效果等同于卡片上的停止按钮。 |
| `/stop <scope>` | 按 scope 中断指定运行，例如 `comment:<hash>` 或 `meeting:<id>`（仅管理员）。 |
| `/help` | 显示可用命令列表。 |

说明：

- 私聊不需要 `@`；群聊默认需要。可以把 `access.requireMentionInGroup` 设为 `false` 放宽。
- `/config set`、`/meeting` 和 `/stop <scope>` 是管理员命令。发送者在 `access.admins` 中可以通过；`admins` 为空时，`access.allowedUsers` 作为管理员名单；运行时应用 owner 也可以通过。owner 查询失败时，非管理员一律拒绝。
- `/config` 的读取对所有已经通过消息策略的发送者开放。
- 命令会绕过防抖队列，并丢弃该 scope（聊天或话题）中已经排队的消息。
- `/stop <scope>` 用于中断没有聊天 scope 的任务；`/status` 会列出当前正在运行的 scope。
- `maxConcurrentRuns` 限制 IM、评论和会议任务的总并发。达到上限时，IM 消息继续排队；评论和会议任务会短暂等待运行槽，超时后回复提示。
- `/meeting` 还要求 profile 的 `meeting.enabled` 为 `true`，并且应用具备 VC bot 相关 scope。
- 命令回复都是文本消息，不是交互卡片。

## 回复模式

`/config set messageReply` 控制最终答案的发送方式：

- **`card`**：创建一张 CardKit 流式卡片，agent 运行期间持续更新。卡片展示答案和精简工具摘要，运行中还有停止按钮。超过约 8.5 分钟的长任务会接到一张新卡片继续展示。
- **`markdown`**：默认值。当前 Rust 实现也走卡片渲染；卡片无法继续更新时回退为普通消息。
- **`text`**：不创建卡片，run 结束后发送一条纯文本消息。

`showToolCalls` 控制最终卡片或文本回复里是否展示工具摘要，不影响 agent 实际执行的操作。

飞书会在流式卡片创建约 10 分钟后强制结束卡片并丢弃后续更新。larkline 会更新到约 8.5 分钟，然后创建一张新卡片，在旧卡片上标注 `⏭ 过程已接续到下一张卡片`，新卡片以 `↪ 接续上一张卡片` 开头继续展示；最终答案写在最新卡片上。如果新卡片创建失败，才回退为停止更新、任务结束后单独发送最终答案。这是服务端限制，桌面端和手机端都一样。

### 并发

`preferences.maxConcurrentRuns`（默认 10，限制在 1–50）限制 IM、评论和会议 scope 同时运行的 agent 任务数。`/config set maxConcurrentRuns <1-50>` 会写入当前 profile 并立即生效。把上限降到低于当前运行数时，已有任务会继续跑完，新任务等到有空槽后再启动。

## 配置

### Profile

一个 profile 就是一套 bot 身份：飞书 / Lark 应用凭据、agent 类型、展示偏好、访问名单、会话和工作目录。`config.json` 使用 schema v2，包含 `activeProfile` 和 `profiles` 映射。单条命令可以用 `--profile`（或 `LARKLINE_PROFILE`）指定 profile，否则使用当前 profile。

下面只是 profile 里的字段片段，不要整段覆盖 `config.json`；请改对应 profile。

```json
{
  "agentKind": "claude",
  "accounts": {
    "app": {
      "id": "cli_xxx",
      "tenant": "feishu",
      "secret": {
        "source": "exec",
        "provider": "bridge",
        "id": "app-cli_xxx"
      }
    }
  },
  "preferences": {
    "messageReply": "card",
    "showToolCalls": true,
    "maxConcurrentRuns": 10
  },
  "access": {
    "allowedUsers": ["ou_xxx"],
    "requireMentionInGroup": true,
    "admins": ["ou_xxx"]
  },
  "workspaces": {
    "default": "/Users/me/projects/app"
  }
}
```

Rust 二进制会保留自己暂时不读取的配置字段，因此与 TypeScript 版本往返读写不会丢设置。

### App Secret 与 keystore

`profile create` 和 `onboard` 会把 App Secret 用 AES-256-GCM 加密后写入 profile 的 `secrets.enc`。配置里只保存引用（`source: "exec"`、`provider: "bridge"`、`id: "app-<app_id>"`），明文不会出现在 `config.json`。

```bash
larkline secrets list                       # 只列 id
larkline secrets set app-cli_xxx            # 提示输入，不回显
larkline secrets remove app-cli_xxx
```

加密密钥由机器 hostname、用户名和 profile 私有的 `.keystore.salt` 派生。把 `secrets.enc` 单独拷到另一台机器或另一个用户下通常无法解密；需要重新录入 App Secret。

### 环境变量

| 变量 | 作用 |
|---|---|
| `LARKLINE_HOME` | 状态根目录，默认 `~/.larkline`；`--home` 优先。 |
| `LARKLINE_PROFILE` | 当前进程使用的 profile；`--profile` 优先。 |
| `LARKLINE_CALLBACK_SECRET` | 校验 agent 渲染的卡片按钮时使用的 HMAC key，可选覆盖。未配置时默认使用 profile 的 App Secret 作为 key version 1（与 TypeScript 一致）。 |

改名前的回退变量 `LARK_CHANNEL_HOME`、`LARK_CHANNEL_PROFILE`、`LARK_CHANNEL_CALLBACK_SECRET` 在新变量不存在时仍然生效。

卡片回调的 key 默认使用 profile 的 App Secret，因此 bridge-aware `lark-cli` 签发的 token 无需额外配置即可校验。显式配置的 `callback.secret`（密钥轮换时用 `callback.keys`）或 `LARKLINE_CALLBACK_SECRET` 仍然优先。bridge 和负责签名卡片的客户端必须使用同一个值。

### 工作目录

profile 可以通过 `workspaces.default` 指定默认工作目录。没有指定时，bridge 使用 profile 托管的默认工作目录 `<state-root>-workspaces/<profile>/default`（默认 home 下即 `~/.larkline-workspaces/<profile>/default`），并在需要时创建。

工作目录只是 agent run 的当前目录，不是文件系统 sandbox；agent 实际能访问哪些文件仍取决于它自己的权限。

### 权限与授权

profile 的 `permissions` 块决定 agent 的访问级别，以及工具调用是否需要用户授权：

```json
{
  "permissions": {
    "defaultAccess": "workspace",
    "maxAccess": "workspace",
    "claude": { "permissionMode": "default" }
  }
}
```

- **Claude Code** 在 `default` 或 `acceptEdits` 下使用 stdio 权限询问协议。工具调用会发来一张独立卡片，展示命令/路径，并可展开参数。点「允许本次」会原样放行；点「本次会话都允许」会在 CLI 提供会话建议时发送；点「拒绝」会告诉模型用户拒绝了该操作。`plan` 和 `bypassPermissions` 不会询问，保持原来的一次性 stdin 路径。参数在 12KB 卡片预算内完整展示，超出时会带明显提示截断。
- **Codex** 在 `read-only` 或 `workspace-write` 下改用 `codex app-server`（`codex exec` 协议没有反向授权通道）。命令执行、文件修改和额外权限请求都走同一张授权卡片。`danger-full-access` 仍然使用 `codex exec`，不会询问。
- **用户选择题**（`AskUserQuestion` 和 Codex `request_user_input`）走同一套卡片：选项变成按钮；多个问题逐个回答；多选问题提供提交按钮。答案会按 agent 原生格式回传。
- 只有本轮第一条消息的发送者可以点击按钮；其他人的点击会被拒绝。10 分钟未处理会自动拒绝；如果卡片发送失败，也会立即拒绝，避免 agent 一直卡住。授权/问题卡片是普通非流式消息，不受 8.5 分钟流式卡片限制影响。
- 卡片发在当前聊天里，所以其他群成员也能看到命令和工具参数（只有发起人能点击）。如果工具参数敏感，请不要在群里做授权。
- 评论和会议任务没有授权卡片流程，保持 agent 的非交互行为；如果这些场景需要执行工具，请为对应 profile 使用 full access。
- 需要较新的 Claude Code（支持 stdio 权限询问协议）或较新的 Codex（支持 `codex app-server`）。`larkline doctor` 不会探测授权协议；如果本地 CLI 版本太旧，run 会直接报出 CLI 的错误，而不是静默自动放行。

### 访问控制

Rust bridge 目前实现了 TypeScript 访问模型的一个子集：

- `access.allowedUsers` 限制谁能在私聊、群聊和评论线程中触发 bot。名单为空时，不启用用户白名单。
- 群消息默认要求 `@` mention；`access.requireMentionInGroup` 可以按 profile 关闭这个要求。
- `access.admins` 控制 `/config set`、`/meeting` 和 `/stop <scope>`；`admins` 为空时，`access.allowedUsers` 作为管理员集合。运行时应用 owner 也可以通过；owner 从 Applications API 解析并缓存 30 分钟。
- `access.allowedChats` 暂不作用于普通消息，只在校验卡片按钮点击时使用。

`/invite` 和 `/remove` 已用于维护用户、管理员和群名单。普通消息上的 TypeScript owner 绕过、team 模式的普通消息放行、按聊天的 `chatRequireMention` 覆盖尚未生效；`allowedChats` 只用于卡片按钮校验。特别是：`allowedUsers` 为空时，任何能触达应用的人都可以私聊 bot。对外分享 bot 前请显式配置 `allowedUsers`。

### 会议

会议智能体是可选能力。在 profile 中加入会议配置并重启：

```json
{
  "meeting": {
    "enabled": true
  }
}
```

应用需要具备 VC bot 相关 scope，租户需要支持 VC bot 能力。启用后可以用 `/meeting join <9 位会议号>` 入会；会中聊天带配置的 trigger 前缀即可提问，`/meeting notes` 可以总结字幕。具体限制见 [Rust 移植说明](./docs/RUST-PORT.md)。

## 数据目录与兼容性

| 路径 | 内容 |
|---|---|
| `~/.larkline/config.json` | root config：`activeProfile` 和 `profiles` |
| `~/.larkline/profiles/<profile>/sessions.json` | 每个聊天/话题 scope 的可恢复会话 |
| `~/.larkline/profiles/<profile>/workspaces.json` | 按 scope 保存的工作目录覆盖 |
| `~/.larkline/profiles/<profile>/pending.json` | 持久化的按 scope 排队消息（逐消息 24 小时 TTL，权限 0600） |
| `~/.larkline/profiles/<profile>/secrets.enc` | 加密的 App Secret 和其他 secret |
| `~/.larkline/profiles/<profile>/.keystore.salt` | 每个 profile 的 keystore salt |
| `~/.larkline/profiles/<profile>/logs/daemon/` | daemon 的 stdout / stderr 日志 |
| `~/.larkline/registry/processes.json` | `ps` / `kill` 读取的进程注册表 |
| `~/.larkline/secrets-getter` | 供 lark-cli 使用的 exec-provider wrapper |

### 同一份配置，同一个 keystore

- 状态布局、配置 schema v2 和加密 keystore 与 TypeScript 版本共用。
- keystore 使用相同的 AES-256-GCM / PBKDF2-HMAC-SHA256 构造，因此 Rust 二进制可以解密 TypeScript 版本写入的 App Secret，反过来也一样。
- 使用默认 home 首次启动时，如果 `~/.larkline` 不存在，会自动把 `~/.lark-channel` rename 为 `~/.larkline`；`~/.lark-channel-workspaces` 也会尽量一并迁移。
- 两个实现可以读取同一份配置。切换前先停掉其中一个；不要同时用两个实现连接同一个应用和 profile。

### 从 TypeScript 版本迁移

1. 用旧 CLI 停止并注销旧服务：`larkline stop`、`larkline unregister`。
2. 用 `install.sh` 安装 Rust 二进制，并确认 `command -v larkline` 指向它。
3. 运行 `larkline doctor`。它会找到原有 profile、app id、keystore 和会话。
4. 用 `larkline start` 启动服务。

如果 `~/.lark-channel` 和 `~/.larkline` 同时存在，不会做 rename，已有的 `~/.larkline` 优先。不需要重新扫码，也不需要重新输入 App Secret。

### 剩余缺口

bridge 的出站 transport 可注入（`BridgeTransport`），并且有端到端 run-flow 测试
（`crates/larkline-bridge/tests/run_flow.rs`），覆盖正常流程、`/stop`、空闲超时、
markdown 模式、附件下载失败、8.5 分钟卡片接续，以及 Claude / Codex 授权卡片点击。
目前只剩以下项目：

- **Apple 发布 secrets**：macOS 发布流水线需要 Developer ID 签名和 notarize
  凭据，目前仍待配置；Windows Authenticode 签名也未配置。
- **Wiki 评论**：`wiki` 评论目标目前只记录日志并跳过；已支持 `doc`、`docx`、
  `sheet` 和 `file`。
- **会议纪要 thread 归属**：纪要会作为顶层消息发到配置的来源聊天或 owner
  私聊，不会发进会议来源话题。
- **Session catalog**：不读也不写 `sessions.json.catalog.json`；恢复会话只按
  scope 和工作目录匹配。
- **授权范围**：授权和问题卡片只接入了 IM run loop；评论和会议任务仍保持原来的
  非交互行为。暂无编辑工具参数；没有选项、需要自由文本的问题会带提示跳过；群里
  只有本轮第一条消息的发送者可以操作。
- **非 TS 附加项与剩余边界**：普通消息上尚未生效的访问控制边界（TypeScript
  owner 绕过、team 模式的普通消息放行、按聊天的 `chatRequireMention` 覆盖、
  普通消息上的 `allowedChats`）、Windows Authenticode 签名、Rust `ps` 条目的
  bot 名称补全，以及在线 keepalive watchdog（`lark-channel` 0.6 没有连接状态或
  强制重连接口）。

每项缺口的原因和当前状态见 [Rust 移植说明](./docs/RUST-PORT.md)。

## 常见问题

**bot 没反应，或者本机 agent 一直不回复。** 先运行 `larkline doctor`。然后确认 agent CLI 已安装并登录，再发 `/status` 查看当前 cwd 和会话是否可恢复。`/new` 通常可以解决过期会话。

**卡片停在某一帧，之后收不到答案。** 如果 run 已经超过约 8.5 分钟，先找同聊天里的新卡片：bridge 会把进度接到新卡片，最终答案写在新卡片上。只有新卡片创建失败时，才会回退为任务结束后单独发送最终答案。

**可以让一个 bot 用 Claude Code，另一个用 Codex 吗？** 可以。用不同的 `--agent` 创建两个 profile，然后各跑一个服务。agent 类型在创建 profile 时确定；需要换类型时请新建 profile。

**怎么更换 App Secret？** 运行 `larkline secrets set app-<app_id>`，然后重启 bridge 或服务。配置里的引用不需要改。

**Windows 提示 SmartScreen 警告。** Windows 二进制目前没有代码签名。配置好 Apple secrets 后，macOS 产物会做 Developer ID 签名并 notarize（目前仍待配置）；Linux 版本未签名，但提供校验和。

**`larkline ps` 里有 stale 条目。** 被 SIGKILL 杀掉或崩溃的进程没法自己注销。下一次 `larkline run` 会清理死 pid；在那之前 `ps` 会把它们标成 `(stale)`，`kill` 不会向它们发信号。

**怎么卸载？** 先 `larkline stop`，再 `larkline unregister`，删除 `larkline` 二进制；如果也要删除本地状态，再删除 `~/.larkline`。

## 开发与发布

在仓库根目录构建和测试：

```bash
cargo build --release
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
./target/release/larkline doctor
```

需要 Rust 1.85 或更新版本。CI 在 macOS、Ubuntu 和 Windows 上跑测试；格式检查和 clippy 只在 Linux 上跑。发布由 tag 触发：推送 `v*` tag 后会运行发布流水线，执行格式检查和测试，构建静态链接（musl）的 Linux x86_64/aarch64、macOS Universal（Developer ID 签名并 notarize；缺少 Apple secrets 时流水线会失败，目前仍待配置）和 Windows x86_64，然后上传产物、`SHA256SUMS` 并创建 GitHub Release。

## 许可

[MIT](./LICENSE)

<img src="./assets/feedback-group-qr.png" alt="飞书反馈群二维码" width="360">
