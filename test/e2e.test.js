import assert from 'node:assert/strict';
import childProcess from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const cliPath = process.env.SAFEDEV_BIN || path.join(repoRoot, 'target', 'debug', 'safedev');

function ensureBuilt() {
  const build = childProcess.spawnSync('cargo', ['build', '--quiet'], {
    cwd: repoRoot,
    encoding: 'utf8'
  });
  assert.equal(build.status, 0, build.stderr || build.stdout);
}

function makeTempDir(name) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${name}-`));
}

function writeFakeLimactl(dir) {
  const fakePath = path.join(dir, 'limactl');
fs.writeFileSync(fakePath, `#!/usr/bin/env node
const fs = require('node:fs');
const log = process.env.SAFEDEV_FAKE_LIMA_LOG;
const argv = process.argv.slice(2);
if (log) {
  fs.appendFileSync(log, JSON.stringify({
    argv,
    cwd: process.cwd()
  }) + '\\n');
}
if (argv[0] === 'list' && process.env.SAFEDEV_FAKE_LIMA_INSTANCE) {
  process.stdout.write(process.env.SAFEDEV_FAKE_LIMA_INSTANCE + ' Running\\n');
  process.exit(0);
}
if (process.env.SAFEDEV_FAKE_LIMA_ALREADY_EXISTS === '1' && argv[0] === 'start' && argv.includes('--name')) {
  const name = argv[argv.indexOf('--name') + 1];
  process.stderr.write('instance "' + name + '" already exists\\n');
  process.exit(1);
}
process.exit(0);
`);
  fs.chmodSync(fakePath, 0o755);
  return fakePath;
}

function runSafeDev(args, env, cwd) {
  return childProcess.spawnSync(cliPath, args, {
    cwd,
    env: { ...process.env, ...env },
    encoding: 'utf8'
  });
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function workspaceState(home) {
  const workspaces = path.join(home, 'workspaces');
  const entries = fs.readdirSync(workspaces);
  assert.equal(entries.length, 1);
  return readJson(path.join(workspaces, entries[0], 'state.json'));
}

test('SafeDev CLI exercises the product flow end to end with a Lima backend', () => {
  ensureBuilt();
  const temp = makeTempDir('safedev-e2e');
  const project = path.join(temp, 'sample-project');
  const home = path.join(temp, 'safedev-home');
  const fakeLog = path.join(temp, 'limactl.log');
  const fakeLimactl = writeFakeLimactl(temp);
  fs.mkdirSync(path.join(project, '.devcontainer'), { recursive: true });
  fs.mkdirSync(path.join(project, 'crates', 'engine'), { recursive: true });
  fs.mkdirSync(path.join(project, 'services', 'api'), { recursive: true });
  fs.writeFileSync(path.join(project, 'package.json'), '{"packageManager":"pnpm@10.33.0","scripts":{"dev":"vite"},"dependencies":{}}\n');
  fs.writeFileSync(path.join(project, 'index.js'), 'console.log("hello")\n');
  fs.writeFileSync(path.join(project, 'Tiltfile'), 'local_resource("hello", cmd="echo hello")\n');
  fs.writeFileSync(path.join(project, 'crates', 'engine', 'Cargo.toml'), '[package]\nname = "engine"\nversion = "0.1.0"\nedition = "2021"\n');
  fs.writeFileSync(path.join(project, 'services', 'api', 'pyproject.toml'), '[project]\nname = "api"\nversion = "0.1.0"\n');
  fs.writeFileSync(path.join(project, '.devcontainer', 'devcontainer.json'), `{
    // SafeDev should tolerate common JSONC comments.
    "name": "Sample Devcontainer",
    "image": "mcr.microsoft.com/devcontainers/javascript-node:22",
    "remoteUser": "node"
  }\n`);

  const env = {
    SAFEDEV_HOME: home,
    SAFEDEV_LIMACTL: fakeLimactl,
    SAFEDEV_FAKE_LIMA_LOG: fakeLog
  };

  const up = runSafeDev(['up', '--project', project], env, project);
  assert.equal(up.status, 0, up.stderr);
  assert.match(up.stdout, /SafeDev workspace ready/);
  assert.match(up.stdout, /Host home: not mounted/);
  assert.match(up.stdout, /Docker socket: not mounted/);
  assert.match(up.stdout, /Install scripts: prompt before execution/);

  let state = workspaceState(home);
  assert.equal(state.mode, 'normal');
  assert.equal(state.project.vmPath, '/workspaces/sample-project');
  assert.equal(state.devcontainer.name, 'Sample Devcontainer');
  assert.equal(state.projectProfile.toolchains.javascript, true);
  assert.equal(state.projectProfile.toolchains.python, true);
  assert.equal(state.projectProfile.toolchains.rust, true);
  assert.equal(state.projectProfile.toolchains.tilt, true);
  assert.deepEqual(state.projectProfile.packageManagers, ['pnpm@10.33.0']);
  assert.equal(state.projectProfile.manifests.some((manifest) => manifest.path === 'crates/engine/Cargo.toml'), true);
  assert.equal(state.projectProfile.manifests.some((manifest) => manifest.path === 'services/api/pyproject.toml'), true);

  const policy = readJson(state.paths.policyFile);
  assert.equal(policy.filesystem.host_home, false);
  assert.equal(policy.filesystem.docker_socket, false);
  assert.equal(policy.secrets.ambient, false);
  assert.equal(policy.secrets.command_scoped, true);
  assert.equal(policy.network.mode, 'monitored');
  assert.equal(policy.network.block_metadata_ips, true);
  assert.equal(policy.network.log_egress, true);
  assert.equal(policy.packages.install_scripts, 'prompt');
  assert.equal(policy.packages.require_lockfile, 'warn');
  assert.equal(policy.github.credential_mode, 'scoped_ephemeral');
  assert.deepEqual(policy.github.default_permissions, {
    contents: 'read',
    pull_requests: 'write'
  });

  const limaConfig = fs.readFileSync(state.paths.limaFile, 'utf8');
  const realProject = fs.realpathSync(project);
  assert.match(limaConfig, new RegExp(`location: ${JSON.stringify(realProject).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
  assert.match(limaConfig, /mountPoint: "\/workspaces\/sample-project"/);
  assert.match(limaConfig, /guestPort: 3000/);
  assert.match(limaConfig, /guestPort: 5173/);
  assert.doesNotMatch(limaConfig, new RegExp(os.homedir().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.doesNotMatch(limaConfig, /docker\.sock/);
  assert.match(limaConfig, /169\.254\.169\.254/);
  assert.match(limaConfig, /safedev-egress-proxy\.service/);
  assert.match(limaConfig, /export HTTP_PROXY=http:\/\/127\.0\.0\.1:18080/);
  assert.match(limaConfig, /apt-get install -y .*nodejs.*npm/);
  assert.match(limaConfig, /apt-get install -y .*python3/);
  assert.match(limaConfig, /apt-get install -y .*python3-venv/);
  assert.match(limaConfig, /apt-get install -y .*python3-pip/);
  assert.match(limaConfig, /apt-get install -y .*cargo/);
  assert.match(limaConfig, /apt-get install -y .*rustc/);
  assert.match(limaConfig, /npm install -g corepack pnpm yarn/);
  assert.match(limaConfig, /corepack prepare pnpm@10\.33\.0 --activate/);
  assert.match(limaConfig, /python3 -m pip install --break-system-packages --upgrade uv virtualenv/);
  assert.match(limaConfig, /raw\.githubusercontent\.com\/tilt-dev\/tilt/);
  assert.match(limaConfig, /project-profile\.json/);

  const brokerPolicy = readJson(path.join(state.paths.credentialsDir, 'broker-policy.json'));
  assert.equal(brokerPolicy.codex.hostCodexMount, false);
  assert.equal(brokerPolicy.codex.defaultDuration, '2h');
  assert.equal(brokerPolicy.github.credentialMode, 'scoped_ephemeral');
  assert.deepEqual(brokerPolicy.github.defaultPermissions, { contents: 'read', pull_requests: 'write' });

  const secondUp = runSafeDev(['up', '--project', project], {
    ...env,
    SAFEDEV_FAKE_LIMA_ALREADY_EXISTS: '1',
    SAFEDEV_FAKE_LIMA_INSTANCE: state.instanceName
  }, project);
  assert.equal(secondUp.status, 0, secondUp.stderr);
  assert.match(secondUp.stdout, /SafeDev workspace already running/);
  assert.match(secondUp.stdout, /SafeDev workspace ready/);

  const unconfirmedInstall = runSafeDev(['run', '--project', project, 'pnpm', 'install'], env, project);
  assert.notEqual(unconfirmedInstall.status, 0);
  assert.match(unconfirmedInstall.stderr, /Re-run with --yes/);

  const install = runSafeDev(['run', '--project', project, '--yes', 'pnpm', 'install'], env, project);
  assert.equal(install.status, 0, install.stderr);
  assert.match(install.stdout, /Snapshot created:/);
  assert.match(install.stderr, /no package lockfile found/);
  state = workspaceState(home);
  assert.equal(state.snapshots.length, 1);
  assert.equal(state.snapshots[0].reason, 'pre-install');
  assert.equal(fs.existsSync(path.join(state.snapshots[0].projectPath, 'package.json')), true);

  const inspectRun = runSafeDev(['inspect', 'last', '--project', project], env, project);
  assert.equal(inspectRun.status, 0, inspectRun.stderr);
  assert.match(inspectRun.stdout, /SafeDev inspect: last/);
  assert.match(inspectRun.stdout, /Command: pnpm install/);
  assert.match(inspectRun.stdout, /Network: monitored; metadata blocked=true; egress logging=true/);
  assert.match(inspectRun.stdout, /File writes: confined to \/workspaces\/sample-project; host home mounted=false; docker socket mounted=false/);

  fs.writeFileSync(path.join(project, 'evil.txt'), 'created after snapshot\n');
  fs.writeFileSync(path.join(project, 'package.json'), '{"mutated":true}\n');
  const rollback = runSafeDev(['rollback', '--project', project, '--yes'], env, project);
  assert.equal(rollback.status, 0, rollback.stderr);
  assert.match(rollback.stdout, /Rolled back to snapshot:/);
  assert.equal(fs.existsSync(path.join(project, 'evil.txt')), false);
  assert.equal(fs.readFileSync(path.join(project, 'package.json'), 'utf8'), '{"packageManager":"pnpm@10.33.0","scripts":{"dev":"vite"},"dependencies":{}}\n');

  const codex = runSafeDev(['codex', '--project', project, '--', '--version'], env, project);
  assert.equal(codex.status, 0, codex.stderr);
  state = workspaceState(home);
  const codexConfig = fs.readFileSync(path.join(state.paths.codexDir, 'config.toml'), 'utf8');
  assert.match(codexConfig, /sandbox_mode = "workspace-write"/);
  assert.match(codexConfig, /approval_policy = "on-request"/);
  assert.match(codexConfig, /writable_roots = \["\/workspaces\/sample-project"\]/);
  assert.equal(fs.existsSync(path.join(state.paths.codexDir, 'broker.json')), true);
  assert.equal(fs.existsSync(path.join(state.paths.codexDir, 'auth.json')), false);

  const destroy = runSafeDev(['destroy', '--project', project, '--yes'], env, project);
  assert.equal(destroy.status, 0, destroy.stderr);
  assert.match(destroy.stdout, /Destroyed SafeDev workspace:/);
  assert.equal(fs.existsSync(state.paths.root), false);

  const backendCalls = fs.readFileSync(fakeLog, 'utf8').trim().split('\n').map((line) => JSON.parse(line).argv);
  assert.deepEqual(backendCalls[0].slice(0, 4), ['start', '--tty=false', '--name', state.instanceName]);
  assert.equal(backendCalls.some((argv) => argv[0] === 'shell' && argv.some((arg) => arg.includes('pnpm install'))), true);
  assert.equal(backendCalls.some((argv) => argv[0] === 'shell' && argv.some((arg) => arg.includes('HTTP_PROXY=http://127.0.0.1:18080'))), true);
  assert.equal(backendCalls.some((argv) => argv[0] === 'copy' && argv[2] === `${state.instanceName}:/tmp/safedev-codex-${state.id}`), true);
  assert.equal(backendCalls.some((argv) => argv[0] === 'shell' && argv.some((arg) => arg.includes('sudo rm -rf /home/dev/.codex'))), true);
  assert.equal(backendCalls.some((argv) => argv[0] === 'shell' && argv.some((arg) => arg.includes('sudo -H -u dev env'))), true);
  assert.equal(backendCalls.some((argv) => argv[0] === 'shell' && argv.some((arg) => arg.includes('npm install -g @openai/codex'))), true);
  assert.equal(backendCalls.some((argv) => argv[0] === 'shell' && argv.some((arg) => arg.includes('codex --version'))), true);
  assert.deepEqual(backendCalls.at(-1), ['delete', '-f', state.instanceName]);
});

test('SafeDev modes shape locked and trusted policy defaults', () => {
  ensureBuilt();
  for (const mode of ['locked', 'trusted']) {
    const temp = makeTempDir(`safedev-${mode}`);
    const project = path.join(temp, 'repo');
    const home = path.join(temp, 'home');
    const fakeLog = path.join(temp, 'limactl.log');
    const fakeLimactl = writeFakeLimactl(temp);
    fs.mkdirSync(project, { recursive: true });
    fs.writeFileSync(path.join(project, 'package.json'), '{}\n');
    if (mode === 'locked') fs.writeFileSync(path.join(project, 'package-lock.json'), '{"lockfileVersion":3}\n');

    const env = {
      SAFEDEV_HOME: home,
      SAFEDEV_LIMACTL: fakeLimactl,
      SAFEDEV_FAKE_LIMA_LOG: fakeLog
    };

    const up = runSafeDev(['up', '--mode', mode, '--project', project], env, project);
    assert.equal(up.status, 0, up.stderr);
    const state = workspaceState(home);
    const policy = readJson(state.paths.policyFile);
    assert.equal(policy.filesystem.host_home, false);
    assert.equal(policy.filesystem.docker_socket, false);

    if (mode === 'locked') {
      assert.equal(policy.network.mode, 'restricted');
      assert.equal(policy.packages.install_scripts, 'block');
      assert.equal(policy.packages.require_lockfile, 'error');

      const install = runSafeDev(['run', '--project', project, '--yes', 'npm', 'install'], env, project);
      assert.equal(install.status, 0, install.stderr);
      assert.match(install.stdout, /Install lifecycle scripts blocked by policy/);
      const backendCalls = fs.readFileSync(fakeLog, 'utf8').trim().split('\n').map((line) => JSON.parse(line).argv);
      assert.equal(backendCalls.some((argv) => argv.some((arg) => arg.includes('npm install --ignore-scripts'))), true);
    } else {
      assert.equal(policy.network.mode, 'broad_monitored');
      assert.equal(policy.filesystem.sandbox_home_persistent, true);
      assert.equal(policy.filesystem.cache_persistent, true);
      assert.equal(policy.packages.install_scripts, 'prompt');
    }
  }
});
