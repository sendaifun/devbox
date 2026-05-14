import assert from 'node:assert/strict';
import childProcess from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const cliPath = process.env.SAFEDEV_BIN || path.join(repoRoot, 'target', 'debug', 'safedev');
const runRealLima = process.env.SAFEDEV_REAL_LIMA === '1';

function ensureBuilt() {
  const build = childProcess.spawnSync('cargo', ['build', '--quiet'], {
    cwd: repoRoot,
    encoding: 'utf8'
  });
  assert.equal(build.status, 0, build.stderr || build.stdout);
}

function runSafeDev(args, env, cwd, timeout = 600_000) {
  return childProcess.spawnSync(cliPath, args, {
    cwd,
    env: { ...process.env, ...env },
    encoding: 'utf8',
    timeout
  });
}

test('SafeDev boots and runs a command with real Lima', { skip: !runRealLima, timeout: 900_000 }, () => {
  ensureBuilt();
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'safedev-real-lima-'));
  const project = path.join(temp, 'repo');
  const home = path.join(temp, 'home');
  fs.mkdirSync(project, { recursive: true });
  fs.writeFileSync(path.join(project, 'package.json'), '{}\n');
  fs.writeFileSync(path.join(project, 'package-lock.json'), '{"lockfileVersion":3}\n');

  const env = {
    SAFEDEV_HOME: home,
    SAFEDEV_ASSUME_YES: '1'
  };

  let state = null;
  try {
    const up = runSafeDev(['up', '--project', project], env, project);
    assert.equal(up.status, 0, up.stderr || up.stdout);
    assert.match(up.stdout, /SafeDev workspace ready/);

    const workspace = path.join(home, 'workspaces');
    const entries = fs.readdirSync(workspace);
    assert.equal(entries.length, 1);
    state = JSON.parse(fs.readFileSync(path.join(workspace, entries[0], 'state.json'), 'utf8'));

    const run = runSafeDev(['run', '--project', project, 'sh', '-lc', 'pwd && test "$PWD" = "/workspaces/repo"'], env, project);
    assert.equal(run.status, 0, run.stderr || run.stdout);
    assert.match(run.stdout, /\/workspaces\/repo/);

    const inspect = runSafeDev(['inspect', 'last', '--project', project], env, project);
    assert.equal(inspect.status, 0, inspect.stderr);
    assert.match(inspect.stdout, /SafeDev inspect: last/);
    assert.match(inspect.stdout, /Command: sh -lc pwd && test "\$PWD" = "\/workspaces\/repo"/);
  } finally {
    if (state || fs.existsSync(path.join(home, 'workspaces'))) {
      runSafeDev(['destroy', '--project', project, '--yes'], env, project, 300_000);
    }
  }
});
