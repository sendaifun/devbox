# SafeDev Product Shape

SafeDev is a VM-backed devcontainer runtime for running untrusted repos and AI agents without exposing the host machine.

## Product Promise

```text
Clone unknown code. Open it in SafeDev. Run installs, dev servers, and Codex.
If the repo is malicious, it burns the sandbox, not your Mac.
```

## Core UX

```bash
brew install safedev

git clone https://github.com/org/project
cd project

safedev up          # creates isolated VM workspace
safedev codex       # runs Codex inside it
safedev run pnpm install
safedev run pnpm dev
safedev destroy     # deletes VM/home/cache/secrets
```

First run should feel like this:

```text
SafeDev workspace ready

Backend: Lima VM on Apple Virtualization.framework
Project: /workspaces/project
Home: /home/dev
Host home: not mounted
Docker socket: not mounted
Secrets: none by default
Network: monitored
Install scripts: prompt before execution

Run:
  safedev codex
  safedev shell
  safedev run <command>
```

## Architecture

```text
macOS host
  |
  | safedev CLI
  |
  |-- VM manager: Lima / Apple Virtualization.framework
  |-- policy engine
  |-- credential broker
  |-- network monitor/proxy
  |-- filesystem/snapshot manager
  |
Linux VM per project
  |
  |-- /workspaces/<repo>       writable project mount/copy
  |-- /home/dev                synthetic sandbox home
  |-- devcontainer runtime
  |-- Codex CLI
  |-- package managers
  |-- local dev server
```

Do not mount the host home directory. Do not mount host `~/.ssh`, `~/.config/gh`, `~/.aws`, `~/.npmrc`, browser profiles, or `~/.codex` directly.

## Codex Integration

The main command should be:

```bash
safedev codex
```

SafeDev should install/run Codex inside the VM and inject a sandbox-scoped Codex config:

```text
/home/dev/.codex/
  auth.json/config      generated or brokered
  config.toml          SafeDev profile
  memories/            optional project-scoped memory
```

Credential model:

```text
host Codex auth / user login
  -> SafeDev credential broker
  -> short-lived sandbox Codex session
  -> VM-local Codex config
```

The key rule: Codex works normally inside the repo, but cannot see the Mac.

Inside the VM, Codex can use a more ergonomic sandbox profile because SafeDev already owns the host boundary:

```toml
sandbox_mode = "workspace-write"
writable_roots = ["/workspaces/project"]
approval_policy = "on-request"
```

Avoid making users understand nested sandboxes. Product copy should be:

```text
SafeDev protects your Mac. Codex works inside SafeDev.
```

## Security Defaults

Default mode should be strict enough for unknown repos:

```yaml
filesystem:
  host_home: false
  project_writable: true
  sandbox_home_persistent: false
  docker_socket: false

secrets:
  ambient: false
  command_scoped: true
  default_duration: 2h

network:
  mode: monitored
  block_metadata_ips: true
  log_egress: true

packages:
  install_scripts: prompt
  block_new_versions_younger_than: 24h
  require_lockfile: warn

github:
  credential_mode: scoped_ephemeral
  default_permissions:
    contents: read
    pull_requests: write
```

## Modes

```bash
safedev up --mode locked
safedev up --mode normal
safedev up --mode trusted
```

### Locked

For unknown repos. No ambient secrets, install scripts blocked, network heavily restricted.

### Normal

Default mode. Project writable, network monitored, install scripts prompted, explicit secrets.

### Trusted

For owned repos. Persistent cache/home allowed, broader network, still no host home mount by default.

## Important Features

- Per-project VM or VM namespace.
- Disposable sandbox home.
- Snapshot before dependency install.
- `safedev rollback` after suspicious behavior.
- `safedev inspect last` for process tree, file writes, network calls.
- Devcontainer compatibility: consume `.devcontainer/devcontainer.json`.
- No Docker socket passthrough by default.
- Nested Docker only inside the VM when needed.
- Port forwarding: `localhost:3000 -> VM:3000`.
- Editor integration later: VS Code / Cursor Remote SSH into the VM.

## MVP Build Order

1. `safedev up/shell/run/destroy` on macOS using Lima.
2. Isolated home, no host home mount, project mount only.
3. Devcontainer support.
4. `safedev codex` with VM-local Codex install/config.
5. GitHub scoped auth broker.
6. Package lifecycle-script prompting.
7. Snapshots and rollback.
8. Network egress logging/blocking.
9. Linux backend with Firecracker for stronger/cloud isolation.

## Positioning

The strongest version of this product is not "secure Docker." It is a local disposable VM workspace with devcontainer ergonomics and first-class Codex support.
