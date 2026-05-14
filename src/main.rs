use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Write};
use std::os::unix::fs as unix_fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{self, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::Stylize;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Paragraph, Wrap};
use ratatui::Frame;

type CliResult<T = ()> = Result<T, String>;

const NETWORK_PROXY_JS: &str = r#"#!/usr/bin/env node
const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const net = require('node:net');
const { URL } = require('node:url');

const logFile = process.env.SAFEDEV_EGRESS_LOG || '/var/log/safedev/egress.log';
const mode = process.env.SAFEDEV_NETWORK_MODE || 'monitored';
const metadataIps = new Set((process.env.SAFEDEV_BLOCK_METADATA_IPS || '').split(',').filter(Boolean));
const allowlist = new Set((process.env.SAFEDEV_NETWORK_ALLOWLIST || '').split(',').filter(Boolean));

function writeLog(entry) {
  const line = JSON.stringify(Object.assign({ at: new Date().toISOString() }, entry)) + '\n';
  try {
    fs.appendFileSync(logFile, line);
  } catch (error) {
    console.error('safedev-egress-log:', error.message);
  }
}

function normalizedHost(host) {
  return String(host || '').replace(/^\[/, '').replace(/\]$/, '').split(':')[0];
}

function blockReason(host) {
  const clean = normalizedHost(host);
  if (metadataIps.has(clean)) return 'metadata';
  if (mode === 'restricted' && clean !== 'localhost' && clean !== '127.0.0.1' && clean !== '::1' && !allowlist.has(clean)) {
    return 'restricted';
  }
  return null;
}

const server = http.createServer((request, response) => {
  let target;
  try {
    target = new URL(request.url);
  } catch {
    response.writeHead(400);
    response.end('SafeDev proxy expected an absolute URL');
    return;
  }

  const reason = blockReason(target.hostname);
  writeLog({ type: 'http', method: request.method, host: target.hostname, port: target.port || null, url: request.url, blocked: Boolean(reason), reason });
  if (reason) {
    response.writeHead(403);
    response.end('Blocked by SafeDev network policy');
    return;
  }

  const transport = target.protocol === 'https:' ? https : http;
  const upstream = transport.request({
    protocol: target.protocol,
    hostname: target.hostname,
    port: target.port || (target.protocol === 'https:' ? 443 : 80),
    method: request.method,
    path: target.pathname + target.search,
    headers: request.headers
  }, (upstreamResponse) => {
    response.writeHead(upstreamResponse.statusCode || 502, upstreamResponse.headers);
    upstreamResponse.pipe(response);
  });
  upstream.on('error', (error) => {
    response.writeHead(502);
    response.end(error.message);
  });
  request.pipe(upstream);
});

server.on('connect', (request, clientSocket, head) => {
  const parts = request.url.split(':');
  const host = parts[0];
  const port = Number(parts[1] || 443);
  const reason = blockReason(host);
  writeLog({ type: 'connect', host, port, blocked: Boolean(reason), reason });
  if (reason) {
    clientSocket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    clientSocket.destroy();
    return;
  }

  const upstreamSocket = net.connect(port, host, () => {
    clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    if (head.length > 0) upstreamSocket.write(head);
    upstreamSocket.pipe(clientSocket);
    clientSocket.pipe(upstreamSocket);
  });
  upstreamSocket.on('error', () => clientSocket.destroy());
});

server.listen(18080, '127.0.0.1');
"#;

#[derive(Default)]
struct Options {
    project: Option<String>,
    mode: Option<String>,
    yes: bool,
    json: bool,
    rest: Vec<String>,
}

#[derive(Clone)]
struct Devcontainer {
    path: String,
    name: Option<String>,
    image: Option<String>,
    docker_file: Option<String>,
    remote_user: Option<String>,
    post_create_command: Option<String>,
}

#[derive(Clone)]
struct ProjectManifest {
    ecosystem: String,
    path: String,
    package_manager: Option<String>,
}

#[derive(Clone)]
struct ProjectProfile {
    javascript: bool,
    python: bool,
    rust: bool,
    tilt: bool,
    package_managers: Vec<String>,
    manifests: Vec<ProjectManifest>,
}

#[derive(Clone)]
struct Policy {
    mode: String,
    host_home: bool,
    project_writable: bool,
    sandbox_home_persistent: bool,
    cache_persistent: bool,
    docker_socket: bool,
    secrets_ambient: bool,
    secrets_command_scoped: bool,
    default_duration: String,
    network_mode: String,
    block_metadata_ips: bool,
    log_egress: bool,
    blocked_metadata_ips: Vec<String>,
    network_allowlist: Vec<String>,
    install_scripts: String,
    block_new_versions_younger_than: String,
    require_lockfile: String,
    github_credential_mode: String,
    github_contents_permission: String,
    github_pull_requests_permission: String,
}

#[derive(Clone)]
struct Snapshot {
    label: String,
    reason: String,
    created_at: String,
    project_path: String,
}

#[derive(Clone)]
struct StatePaths {
    root: PathBuf,
    state_json: PathBuf,
    state_env: PathBuf,
    policy_file: PathBuf,
    lima_file: PathBuf,
    devcontainer_file: PathBuf,
    project_profile_file: PathBuf,
    codex_dir: PathBuf,
    credentials_dir: PathBuf,
    snapshots_dir: PathBuf,
    snapshots_file: PathBuf,
    rollback_backups_dir: PathBuf,
    history_file: PathBuf,
    last_inspect_file: PathBuf,
    last_inspect_env: PathBuf,
}

#[derive(Clone)]
struct State {
    id: String,
    mode: String,
    created_at: String,
    status: String,
    instance_name: String,
    project_name: String,
    project_host_path: PathBuf,
    project_vm_path: String,
    paths: StatePaths,
    policy_summary_network: String,
    policy_summary_install_scripts: String,
    devcontainer: Option<Devcontainer>,
    project_profile: ProjectProfile,
    snapshots: Vec<Snapshot>,
}

struct InspectEvent {
    action: String,
    command: Vec<String>,
    backend_args: Vec<String>,
    snapshot: Option<Snapshot>,
    codex_config: Option<String>,
    backup_root: Option<String>,
}

enum CodexAuthPlan {
    None,
    ReuseStaged,
    ReuseVm,
    Import {
        path: PathBuf,
        source: &'static str,
        requires_confirmation: bool,
    },
}

#[derive(Clone, Copy, PartialEq)]
enum UpStepStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Clone)]
struct UpStep {
    label: &'static str,
    detail: String,
    status: UpStepStatus,
}

struct UpProgress {
    terminal: Option<ratatui::DefaultTerminal>,
    project: String,
    mode: String,
    steps: Vec<UpStep>,
    logs: VecDeque<String>,
    started_at: Instant,
    tick: usize,
}

#[derive(Clone, Copy)]
enum CommandStream {
    Stdout,
    Stderr,
}

struct CommandChunk {
    stream: CommandStream,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct VmListItem {
    instance_name: String,
    status: String,
    project_name: String,
    project_host_path: String,
    mode: String,
    network: String,
    cpus: String,
    memory: String,
    disk: String,
    live_cpu_percent: String,
    live_memory: String,
    workspace_id: String,
}

#[derive(Clone)]
struct LimaInstanceInfo {
    status: String,
    cpus: String,
    memory: String,
    disk: String,
    driver_pid: Option<String>,
}

#[derive(Clone)]
struct ProcessUsage {
    cpu_percent: String,
    rss: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("safedev: {error}");
        process::exit(1);
    }
}

fn run() -> CliResult {
    let mut argv: Vec<String> = env::args().skip(1).collect();
    if argv.is_empty() || argv[0] == "--help" || argv[0] == "-h" {
        println!("{}", usage());
        return Ok(());
    }

    let command = argv.remove(0);
    match command.as_str() {
        "up" => cmd_up(&argv),
        "shell" => cmd_shell(&argv),
        "run" => cmd_run(&argv),
        "codex" => cmd_codex(&argv),
        "ps" | "list" => cmd_ps(&argv),
        "rollback" => cmd_rollback(&argv),
        "inspect" => cmd_inspect(&argv),
        "destroy" => cmd_destroy(&argv),
        _ => Err(format!("Unknown command \"{command}\".\n\n{}", usage())),
    }
}

fn usage() -> &'static str {
    "SafeDev\n\nUsage:\n  safedev up [--mode locked|normal|trusted] [--project PATH]\n  safedev shell [--project PATH]\n  safedev run [--project PATH] [--yes] <command...>\n  safedev codex [--project PATH] [--yes] [-- <codex-args...>]\n  safedev ps [--json]\n  safedev list [--json]\n  safedev rollback [--project PATH] [--yes]\n  safedev inspect last [--project PATH]\n  safedev destroy [--project PATH] [--yes]\n"
}

fn parse_options(args: &[String]) -> CliResult<Options> {
    let mut options = Options::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--" => {
                options.rest.extend(args[index + 1..].iter().cloned());
                break;
            }
            "--project" | "-C" => {
                index += 1;
                options.project = args.get(index).cloned();
                if options.project.is_none() {
                    return Err("--project requires a path".to_string());
                }
            }
            "--mode" => {
                index += 1;
                options.mode = args.get(index).cloned();
                if options.mode.is_none() {
                    return Err("--mode requires locked, normal, or trusted".to_string());
                }
            }
            "--yes" | "-y" => options.yes = true,
            "--json" => options.json = true,
            value => options.rest.push(value.to_string()),
        }
        index += 1;
    }
    Ok(options)
}

fn cmd_up(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    let requested_mode = options.mode.clone();
    let mode = requested_mode
        .clone()
        .unwrap_or_else(|| "normal".to_string());
    if !["locked", "normal", "trusted"].contains(&mode.as_str()) {
        return Err(format!(
            "Invalid mode \"{mode}\". Expected locked, normal, or trusted."
        ));
    }

    let project_path = resolve_project(options.project.as_deref())?;
    let mut progress = UpProgress::new(&project_path, &mode);
    progress.start(0, "Resolving workspace state");
    let paths = workspace_paths(&project_path);
    let mut state = if paths.state_env.exists() {
        let existing = load_state(options.project.as_deref())?;
        if let Some(requested_mode) = requested_mode {
            if requested_mode != existing.mode {
                return Err(format!(
                    "SafeDev workspace already exists in {} mode. Run \"safedev destroy --yes\" before recreating it in {requested_mode} mode.",
                    existing.mode
                ));
            }
        }
        progress.done(0, "Existing SafeDev workspace found");
        existing
    } else {
        progress.start(1, "Reading devcontainer and project manifests");
        let devcontainer = load_devcontainer(&project_path)?;
        let project_profile = detect_project_profile(&project_path)?;
        progress.done(0, "New SafeDev workspace initialized");
        progress.done(1, profile_summary(&project_profile));
        init_state(project_path, &mode, devcontainer, project_profile)?
    };
    let policy = build_policy(&state.mode);
    progress.start(1, "Detecting project toolchains and manifests");
    if state.devcontainer.is_none() {
        state.devcontainer = load_devcontainer(&state.project_host_path)?;
    }
    state.project_profile = detect_project_profile(&state.project_host_path)?;
    progress.done(1, profile_summary(&state.project_profile));

    progress.start(2, "Writing policy, Lima config, and broker policy");
    write_text(&state.paths.policy_file, &policy_json(&policy))?;
    write_text(
        &state.paths.credentials_dir.join("broker-policy.json"),
        &broker_policy_json(&policy),
    )?;
    if let Some(devcontainer) = &state.devcontainer {
        write_text(
            &state.paths.devcontainer_file,
            &devcontainer_json(devcontainer),
        )?;
    }
    write_text(
        &state.paths.project_profile_file,
        &project_profile_json(&state.project_profile),
    )?;
    write_text(&state.paths.lima_file, &render_lima_config(&state, &policy))?;
    save_state(&state)?;
    progress.done(2, "Sandbox configuration written");

    progress.start(
        3,
        "Starting Lima VM. First boot can download images and install packages.",
    );
    let output = start_instance_with_progress(&state, &mut progress)?;
    if !output.status.success() {
        let combined = output_text(&output);
        if combined.contains("already exists") {
            progress.log("Lima instance already exists; checking current status");
            let status = lima_instance_status(&state.instance_name)?;
            match status.as_deref() {
                Some("Running") => {
                    state.status = "running".to_string();
                    save_state(&state)?;
                    progress.done(3, "VM already running");
                    progress.start(4, "Verifying detected toolchains");
                    ensure_vm_toolchains_with_progress(&state, &mut progress)?;
                    progress.done(4, "Toolchains verified");
                    record_event(
                        &state,
                        InspectEvent {
                            action: "up".to_string(),
                            command: Vec::new(),
                            backend_args: start_instance_args(&state),
                            snapshot: None,
                            codex_config: None,
                            backup_root: None,
                        },
                    )?;
                    progress.done(5, "Workspace ready");
                    progress.finish();
                    print_ready(&state, &policy);
                    return Ok(());
                }
                Some("Stopped") => {
                    progress.start(3, "Starting existing stopped VM");
                    let start_existing =
                        start_existing_instance_with_progress(&state, &mut progress)?;
                    if start_existing.status.success() {
                        state.status = "running".to_string();
                        save_state(&state)?;
                        progress.done(3, "Existing VM started");
                        progress.start(4, "Verifying detected toolchains");
                        ensure_vm_toolchains_with_progress(&state, &mut progress)?;
                        progress.done(4, "Toolchains verified");
                        record_event(
                            &state,
                            InspectEvent {
                                action: "up".to_string(),
                                command: Vec::new(),
                                backend_args: start_existing_instance_args(&state),
                                snapshot: None,
                                codex_config: None,
                                backup_root: None,
                            },
                        )?;
                        progress.done(5, "Workspace ready");
                        progress.finish();
                        print_ready(&state, &policy);
                        return Ok(());
                    }
                    progress.fail(3, "Failed to start existing VM");
                    progress.finish();
                    print_output(&start_existing);
                    process::exit(start_existing.status.code().unwrap_or(1));
                }
                Some(other) => {
                    progress.fail(3, format!("Existing VM status: {other}"));
                    return Err(format!(
                        "SafeDev Lima instance {} already exists with status {other}. Run \"safedev destroy --yes\" to recreate it.",
                        state.instance_name
                    ));
                }
                None => {
                    progress.fail(3, "Could not read existing VM status");
                    return Err(format!(
                        "SafeDev Lima instance {} already exists, but limactl list did not return its status. Run \"limactl list\" or \"safedev destroy --yes\".",
                        state.instance_name
                    ));
                }
            }
        }
        progress.fail(3, "Lima VM startup failed");
        progress.finish();
        print_output(&output);
        process::exit(output.status.code().unwrap_or(1));
    }

    state.status = "running".to_string();
    save_state(&state)?;
    progress.done(3, "VM running and provisioned");
    progress.start(4, "Recording startup metadata");
    record_event(
        &state,
        InspectEvent {
            action: "up".to_string(),
            command: Vec::new(),
            backend_args: start_instance_args(&state),
            snapshot: None,
            codex_config: None,
            backup_root: None,
        },
    )?;
    progress.done(4, "Startup metadata recorded");
    progress.done(5, "Workspace ready");
    progress.finish();
    print_ready(&state, &policy);
    Ok(())
}

fn cmd_shell(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    let state = load_state(options.project.as_deref())?;
    let backend_args = shell_interactive_args(&state);
    record_event(
        &state,
        InspectEvent {
            action: "shell".to_string(),
            command: vec![
                "sudo".to_string(),
                "-H".to_string(),
                "-u".to_string(),
                "dev".to_string(),
                "bash".to_string(),
                "-l".to_string(),
            ],
            backend_args: backend_args.clone(),
            snapshot: None,
            codex_config: None,
            backup_root: None,
        },
    )?;
    let status = run_limactl_inherit(&backend_args)?;
    process::exit(status.code().unwrap_or(0));
}

fn cmd_run(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    if options.rest.is_empty() {
        return Err("safedev run requires a command.".to_string());
    }

    let mut state = load_state(options.project.as_deref())?;
    let policy = read_policy_for_state(&state)?;
    let install_command = is_package_install_command(&options.rest);

    if install_command && policy.install_scripts == "prompt" {
        confirm_or_throw(
            "Package install lifecycle scripts may execute untrusted code.",
            options.yes,
        )?;
    }
    if install_command {
        enforce_lockfile_policy(&state, &policy)?;
    }

    let snapshot = if install_command {
        let snapshot = create_snapshot(&mut state, "pre-install")?;
        save_state(&state)?;
        println!("Snapshot created: {}", snapshot.label);
        Some(snapshot)
    } else {
        None
    };

    let command = enforce_install_script_policy(&options.rest, &policy);
    if install_command && policy.install_scripts == "block" {
        println!("Install lifecycle scripts blocked by policy; running with --ignore-scripts.");
    }

    let env_pairs = network_env(&policy);
    let backend_args = shell_args(&state, &command, &env_pairs);
    record_event(
        &state,
        InspectEvent {
            action: "run".to_string(),
            command: command.clone(),
            backend_args,
            snapshot,
            codex_config: None,
            backup_root: None,
        },
    )?;

    let output = run_in_instance(&state, &command, &env_pairs)?;
    print_output(&output);
    process::exit(output.status.code().unwrap_or(0));
}

fn cmd_codex(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    let state = load_state(options.project.as_deref())?;
    let policy = read_policy_for_state(&state)?;
    let auth_plan = codex_auth_plan(&state)?;
    confirm_codex_auth_import(&auth_plan, options.yes)?;
    let codex_dir = prepare_codex_config(&state, &policy, &auth_plan)?;
    install_codex_config(&state, &codex_dir)?;

    let codex_command = {
        let mut command = vec!["codex".to_string()];
        command.extend(options.rest.clone());
        command
    };
    let env_pairs = network_env(&policy);
    let launch = codex_launch_argv(&options.rest, &env_pairs);
    let backend_args = shell_args(&state, &launch, &[]);
    record_event(
        &state,
        InspectEvent {
            action: "codex".to_string(),
            command: codex_command,
            backend_args: backend_args.clone(),
            snapshot: None,
            codex_config: Some(codex_dir.join("config.toml").to_string_lossy().to_string()),
            backup_root: None,
        },
    )?;

    if options.rest.is_empty() {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return Err(
                "safedev codex needs a real terminal. Run it from an interactive shell, or pass non-interactive args like: safedev codex -- --version"
                    .to_string(),
            );
        }
        let mut interactive_backend_args = backend_args.clone();
        interactive_backend_args.insert(1, "--tty=true".to_string());
        let status = run_limactl_inherit(&interactive_backend_args)?;
        process::exit(status.code().unwrap_or(0));
    } else {
        let output = run_in_instance(&state, &launch, &[])?;
        print_output(&output);
        process::exit(output.status.code().unwrap_or(0));
    }
}

fn cmd_ps(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    if !options.rest.is_empty() {
        return Err("safedev ps does not take positional arguments.".to_string());
    }
    let items = load_vm_list_items()?;
    if options.json {
        println!("{}", vm_list_json(&items));
        return Ok(());
    }
    if io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && env::var("SAFEDEV_NO_TUI").ok().as_deref() != Some("1")
    {
        run_vm_list_tui(&items)?;
    } else {
        print_vm_list_table(&items);
    }
    Ok(())
}

fn cmd_rollback(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    let state = load_state(options.project.as_deref())?;
    confirm_or_throw(
        "Rollback will replace project files with the last SafeDev snapshot.",
        options.yes,
    )?;
    let (snapshot, backup_root) = restore_snapshot(&state, None)?;
    save_state(&state)?;
    record_event(
        &state,
        InspectEvent {
            action: "rollback".to_string(),
            command: Vec::new(),
            backend_args: Vec::new(),
            snapshot: Some(snapshot.clone()),
            codex_config: None,
            backup_root: Some(backup_root.to_string_lossy().to_string()),
        },
    )?;
    println!("Rolled back to snapshot: {}", snapshot.label);
    println!("Previous project copy: {}", backup_root.to_string_lossy());
    Ok(())
}

fn cmd_inspect(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    let target = options.rest.first().map(String::as_str).unwrap_or("last");
    if target != "last" {
        return Err("Only \"safedev inspect last\" is implemented.".to_string());
    }
    let state = load_state(options.project.as_deref())?;
    println!("{}", format_last_inspect(&state)?);
    Ok(())
}

fn cmd_destroy(args: &[String]) -> CliResult {
    let options = parse_options(args)?;
    let state = load_state(options.project.as_deref())?;
    confirm_or_throw(
        "Destroy will delete the SafeDev VM and sandbox state for this project.",
        options.yes,
    )?;
    let output = delete_instance(&state)?;
    if !output.status.success() {
        print_output(&output);
        process::exit(output.status.code().unwrap_or(1));
    }
    if state.paths.root.exists() {
        fs::remove_dir_all(&state.paths.root)
            .map_err(|error| format!("failed to remove {}: {error}", state.paths.root.display()))?;
    }
    println!("Destroyed SafeDev workspace: {}", state.instance_name);
    Ok(())
}

fn print_ready(state: &State, policy: &Policy) {
    let install_scripts = if policy.install_scripts == "prompt" {
        "prompt before execution".to_string()
    } else {
        policy.install_scripts.clone()
    };
    println!(
        "SafeDev workspace ready\n\nBackend: Lima VM on Apple Virtualization.framework\nProject: {}\nHome: /home/dev\nHost home: not mounted\nDocker socket: not mounted\nSecrets: none by default\nNetwork: {}\nInstall scripts: {}\n\nRun:\n  safedev codex\n  safedev shell\n  safedev run <command>",
        state.project_vm_path, policy.network_mode, install_scripts
    );
}

fn profile_summary(profile: &ProjectProfile) -> String {
    let mut detected = Vec::new();
    if profile.javascript {
        detected.push("js/ts");
    }
    if profile.python {
        detected.push("python");
    }
    if profile.rust {
        detected.push("rust");
    }
    if profile.tilt {
        detected.push("tilt");
    }
    if detected.is_empty() {
        "No language-specific manifests detected".to_string()
    } else {
        format!(
            "{} detected across {} manifest(s)",
            detected.join(", "),
            profile.manifests.len()
        )
    }
}

fn confirm_or_throw(message: &str, yes: bool) -> CliResult {
    if yes || env::var("SAFEDEV_ASSUME_YES").ok().as_deref() == Some("1") {
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        return Err(format!(
            "{message} Re-run with --yes to confirm in a non-interactive session."
        ));
    }
    print!("{message} Continue? [y/N] ");
    io::stdout().flush().map_err(|error| error.to_string())?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|error| format!("failed to read confirmation: {error}"))?;
    if answer.trim().eq_ignore_ascii_case("y") || answer.trim().eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        Err("Cancelled.".to_string())
    }
}

impl UpProgress {
    fn new(project: &Path, mode: &str) -> Self {
        let terminal = if io::stdin().is_terminal()
            && io::stdout().is_terminal()
            && env::var("SAFEDEV_NO_TUI").ok().as_deref() != Some("1")
        {
            ratatui::try_init().ok()
        } else {
            None
        };
        let mut progress = Self {
            terminal,
            project: project.to_string_lossy().to_string(),
            mode: mode.to_string(),
            steps: vec![
                UpStep {
                    label: "Workspace",
                    detail: String::new(),
                    status: UpStepStatus::Pending,
                },
                UpStep {
                    label: "Project profile",
                    detail: String::new(),
                    status: UpStepStatus::Pending,
                },
                UpStep {
                    label: "Sandbox config",
                    detail: String::new(),
                    status: UpStepStatus::Pending,
                },
                UpStep {
                    label: "VM startup",
                    detail: String::new(),
                    status: UpStepStatus::Pending,
                },
                UpStep {
                    label: "Final checks",
                    detail: String::new(),
                    status: UpStepStatus::Pending,
                },
                UpStep {
                    label: "Ready",
                    detail: String::new(),
                    status: UpStepStatus::Pending,
                },
            ],
            logs: VecDeque::new(),
            started_at: Instant::now(),
            tick: 0,
        };
        progress.draw();
        progress
    }

    fn start(&mut self, index: usize, detail: impl Into<String>) {
        self.set(index, UpStepStatus::Running, detail);
    }

    fn done(&mut self, index: usize, detail: impl Into<String>) {
        self.set(index, UpStepStatus::Done, detail);
    }

    fn fail(&mut self, index: usize, detail: impl Into<String>) {
        self.set(index, UpStepStatus::Failed, detail);
    }

    fn set(&mut self, index: usize, status: UpStepStatus, detail: impl Into<String>) {
        if let Some(step) = self.steps.get_mut(index) {
            step.status = status;
            step.detail = detail.into();
        }
        self.draw();
    }

    fn log(&mut self, line: impl Into<String>) {
        let line = line.into();
        if line.trim().is_empty() {
            return;
        }
        while self.logs.len() >= 8 {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
        self.draw();
    }

    fn pulse(&mut self) {
        self.tick = self.tick.wrapping_add(1);
        self.draw();
    }

    fn enabled(&self) -> bool {
        self.terminal.is_some()
    }

    fn finish(&mut self) {
        if self.terminal.take().is_some() {
            let _ = ratatui::try_restore();
        }
    }

    fn draw(&mut self) {
        let Some(terminal) = self.terminal.as_mut() else {
            return;
        };
        let project = self.project.clone();
        let mode = self.mode.clone();
        let steps = self.steps.clone();
        let logs = self.logs.iter().cloned().collect::<Vec<_>>();
        let elapsed = self.started_at.elapsed();
        let tick = self.tick;
        let _ = terminal
            .draw(|frame| render_up_progress(frame, &project, &mode, &steps, &logs, elapsed, tick));
    }
}

impl Drop for UpProgress {
    fn drop(&mut self) {
        self.finish();
    }
}

fn render_up_progress(
    frame: &mut Frame,
    project: &str,
    mode: &str,
    steps: &[UpStep],
    logs: &[String],
    elapsed: Duration,
    tick: usize,
) {
    let area = frame.area();
    let panel = centered_rect(area, 86, 24);
    let block = Block::bordered()
        .title(Line::from(vec![
            " SafeDev ".bold(),
            "workspace setup ".magenta().bold(),
        ]))
        .border_type(BorderType::Rounded);
    let inner = block.inner(panel);
    frame.render_widget(ratatui::widgets::Clear, panel);
    frame.render_widget(block, panel);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(7),
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(inner);

    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec!["Project ".dim(), project.into()]),
            Line::from(vec![
                "Mode    ".dim(),
                mode.cyan(),
                "   Elapsed ".dim(),
                format!("{}s", elapsed.as_secs()).cyan(),
            ]),
        ]),
        rows[0],
    );

    let step_lines = steps
        .iter()
        .map(|step| {
            let marker = match step.status {
                UpStepStatus::Pending => "[ ]".dim(),
                UpStepStatus::Running => {
                    let frames = ["[-]", "[\\]", "[|]", "[/]"];
                    frames[(tick / 2) % frames.len()].cyan()
                }
                UpStepStatus::Done => "[ok]".green(),
                UpStepStatus::Failed => "[x]".red(),
            };
            let label = match step.status {
                UpStepStatus::Pending => step.label.dim(),
                UpStepStatus::Running => step.label.bold(),
                UpStepStatus::Done => step.label.green(),
                UpStepStatus::Failed => step.label.red().bold(),
            };
            Line::from(vec![
                marker,
                " ".into(),
                label,
                if step.detail.is_empty() {
                    "".into()
                } else {
                    " - ".dim()
                },
                step.detail.clone().dim(),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(step_lines), rows[1]);

    frame.render_widget(Paragraph::new("Latest backend output".dim()), rows[2]);

    let log_lines = if logs.is_empty() {
        vec![Line::from("Waiting for backend output...".dim())]
    } else {
        logs.iter()
            .map(|line| Line::from(line.as_str().dim()))
            .collect::<Vec<_>>()
    };
    frame.render_widget(
        Paragraph::new(log_lines)
            .wrap(Wrap { trim: false })
            .block(Block::bordered().border_type(BorderType::Rounded)),
        rows[3],
    );

    frame.render_widget(
        Paragraph::new(
            "First boot can take a few minutes while Lima downloads and provisions packages.",
        )
        .dim(),
        rows[4],
    );
}

fn codex_auth_plan(state: &State) -> CliResult<CodexAuthPlan> {
    if let Ok(auth_path) = env::var("SAFEDEV_CODEX_AUTH_JSON") {
        let path = PathBuf::from(&auth_path);
        if !path.is_file() {
            return Err(format!(
                "SAFEDEV_CODEX_AUTH_JSON does not point to a file: {auth_path}"
            ));
        }
        return Ok(CodexAuthPlan::Import {
            path,
            source: "SAFEDEV_CODEX_AUTH_JSON",
            requires_confirmation: false,
        });
    }

    if state.paths.codex_dir.join("auth.json").is_file() {
        return Ok(CodexAuthPlan::ReuseStaged);
    }

    if vm_codex_auth_exists(state) {
        return Ok(CodexAuthPlan::ReuseVm);
    }

    if let Ok(home) = env::var("HOME") {
        let path = Path::new(&home).join(".codex").join("auth.json");
        if path.is_file() {
            return Ok(CodexAuthPlan::Import {
                path,
                source: "host ~/.codex/auth.json",
                requires_confirmation: true,
            });
        }
    }

    Ok(CodexAuthPlan::None)
}

fn vm_codex_auth_exists(state: &State) -> bool {
    run_shell_in_instance(state, "sudo test -f /home/dev/.codex/auth.json")
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn confirm_codex_auth_import(plan: &CodexAuthPlan, yes: bool) -> CliResult {
    if let CodexAuthPlan::Import {
        path,
        requires_confirmation: true,
        ..
    } = plan
    {
        if yes || env::var("SAFEDEV_ASSUME_YES").ok().as_deref() == Some("1") {
            return Ok(());
        }
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return Err(format!(
                "{} Re-run with --yes to confirm in a non-interactive session.",
                codex_auth_warning_message(path)
            ));
        }
        run_codex_auth_warning_tui(path)?;
    }
    Ok(())
}

fn codex_auth_warning_message(path: &Path) -> String {
    format!(
        "SafeDev will copy your Codex credential from {} into this project's VM at /home/dev/.codex/auth.json. Code running inside the VM can read that credential. SafeDev will not copy Codex history, logs, sessions, sqlite state, or memories.",
        path.display()
    )
}

fn run_codex_auth_warning_tui(path: &Path) -> CliResult {
    let mut terminal =
        ratatui::try_init().map_err(|error| format!("failed to initialize TUI: {error}"))?;
    let decision = loop {
        if let Err(error) = terminal.draw(|frame| render_codex_auth_warning(frame, path)) {
            break Err(format!("failed to draw TUI: {error}"));
        }
        match event::read() {
            Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Enter => break Ok(true),
                KeyCode::Esc => break Ok(false),
                KeyCode::Char(value) => {
                    let value = value.to_ascii_lowercase();
                    if value == 'y' {
                        break Ok(true);
                    }
                    if value == 'n' || value == 'q' {
                        break Ok(false);
                    }
                    if value == 'c' && key.modifiers.contains(KeyModifiers::CONTROL) {
                        break Ok(false);
                    }
                }
                _ => {}
            },
            Ok(_) => {}
            Err(error) => break Err(format!("failed to read terminal input: {error}")),
        }
    };
    let restore = ratatui::try_restore().map_err(|error| format!("failed to restore TUI: {error}"));
    restore?;
    match decision? {
        true => Ok(()),
        false => Err("Cancelled.".to_string()),
    }
}

fn render_codex_auth_warning(frame: &mut Frame, auth_path: &Path) {
    let area = frame.area();
    let panel = centered_rect(area, 78, 20);
    let block = Block::bordered()
        .title(Line::from(vec![
            " SafeDev ".bold(),
            "Codex credential import ".magenta().bold(),
        ]))
        .border_type(BorderType::Rounded);
    let inner = block.inner(panel);
    frame.render_widget(ratatui::widgets::Clear, panel);
    frame.render_widget(block, panel);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(inner);

    frame.render_widget(
        Paragraph::new(vec![
            Line::from("Codex auth is available on your Mac.".bold()),
            Line::from("SafeDev can copy only the credential needed inside this VM.".dim()),
        ])
        .alignment(Alignment::Center),
        rows[0],
    );

    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                "Source       ".dim(),
                auth_path.display().to_string().cyan(),
            ]),
            Line::from(vec![
                "Destination  ".dim(),
                "/home/dev/.codex/auth.json".cyan(),
            ]),
            Line::from(vec!["Copied       ".dim(), "auth.json only".green()]),
            Line::from(vec![
                "Not copied   ".dim(),
                "history, logs, sessions, sqlite state, memories".dim(),
            ]),
        ]),
        rows[1],
    );

    frame.render_widget(
        Paragraph::new(vec![
            Line::from("Risk".red().bold()),
            Line::from(
                "Code running inside this VM can read that credential. SafeDev still does not mount your host home, browser profile, Docker socket, or full ~/.codex directory.",
            ),
        ])
        .wrap(Wrap { trim: false }),
        rows[2],
    );

    frame.render_widget(
        Paragraph::new("This is a per-project SafeDev VM import. You can also cancel and sign in to Codex inside the VM instead.")
            .wrap(Wrap { trim: false })
            .dim(),
        rows[3],
    );

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            "Enter".cyan().bold(),
            " or ".dim(),
            "y".cyan().bold(),
            " to copy and launch   ".into(),
            "Esc".cyan().bold(),
            ", ".dim(),
            "n".cyan().bold(),
            ", or ".dim(),
            "q".cyan().bold(),
            " to cancel".into(),
        ]))
        .alignment(Alignment::Center),
        rows[4],
    );
}

fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let width = width
        .min(area.width.saturating_sub(2))
        .max(area.width.min(32));
    let height = height
        .min(area.height.saturating_sub(2))
        .max(area.height.min(12));
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(width),
            Constraint::Fill(1),
        ])
        .split(area);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(height),
            Constraint::Fill(1),
        ])
        .split(horizontal[1]);
    vertical[1]
}

fn run_vm_list_tui(items: &[VmListItem]) -> CliResult {
    let mut terminal =
        ratatui::try_init().map_err(|error| format!("failed to initialize TUI: {error}"))?;
    let result = loop {
        if let Err(error) = terminal.draw(|frame| render_vm_list(frame, items)) {
            break Err(format!("failed to draw TUI: {error}"));
        }
        match event::read() {
            Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::Esc | KeyCode::Enter => break Ok(()),
                KeyCode::Char('q') => break Ok(()),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    break Ok(());
                }
                _ => {}
            },
            Ok(_) => {}
            Err(error) => break Err(format!("failed to read terminal input: {error}")),
        }
    };
    let restore = ratatui::try_restore().map_err(|error| format!("failed to restore TUI: {error}"));
    restore?;
    result
}

fn render_vm_list(frame: &mut Frame, items: &[VmListItem]) {
    let area = frame.area();
    let panel = centered_rect(area, 100, 24);
    let block = Block::bordered()
        .title(Line::from(vec![
            " SafeDev ".bold(),
            "VMs ".magenta().bold(),
        ]))
        .border_type(BorderType::Rounded);
    let inner = block.inner(panel);
    frame.render_widget(ratatui::widgets::Clear, panel);
    frame.render_widget(block, panel);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(5),
            Constraint::Length(2),
        ])
        .split(inner);

    let running = items
        .iter()
        .filter(|item| item.status.eq_ignore_ascii_case("running"))
        .count();
    frame.render_widget(
        Paragraph::new(vec![
            Line::from("SafeDev-managed Lima instances".bold()),
            Line::from(vec![
                format!("{} workspace(s)", items.len()).dim(),
                "   ".into(),
                format!("{running} running").green(),
            ]),
        ]),
        rows[0],
    );

    let mut lines = Vec::new();
    let table_width = rows[1].width.saturating_sub(2) as usize;
    let status_width = 10;
    let project_width = 14;
    let live_cpu_width = 7;
    let live_memory_width = 8;
    let alloc_width = 10;
    let separator_width = 10;
    let vm_width = table_width.saturating_sub(
        status_width
            + project_width
            + live_cpu_width
            + live_memory_width
            + alloc_width
            + separator_width,
    );
    lines.push(Line::from(vec![
        fit_cell("STATUS", status_width).dim(),
        "  ".into(),
        fit_cell("PROJECT", project_width).dim(),
        "  ".into(),
        fit_cell("CPU%", live_cpu_width).dim(),
        "  ".into(),
        fit_cell("RSS", live_memory_width).dim(),
        "  ".into(),
        fit_cell("ALLOC", alloc_width).dim(),
        "  ".into(),
        fit_cell("VM", vm_width).dim(),
    ]));
    if items.is_empty() {
        lines.push(Line::from("No SafeDev workspaces found.".dim()));
    } else {
        for item in items {
            lines.push(Line::from(vec![
                status_cell(&item.status, status_width),
                "  ".into(),
                fit_cell(&item.project_name, project_width).into(),
                "  ".into(),
                fit_cell(&item.live_cpu_percent, live_cpu_width).into(),
                "  ".into(),
                fit_cell(&item.live_memory, live_memory_width).into(),
                "  ".into(),
                fit_cell(&format!("{}/{}", item.cpus, item.memory), alloc_width).dim(),
                "  ".into(),
                fit_cell(&item.instance_name, vm_width).cyan(),
            ]));
        }
    }
    frame.render_widget(
        Paragraph::new(lines).block(Block::bordered().border_type(BorderType::Rounded)),
        rows[1],
    );

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            "Enter".cyan().bold(),
            ", ".dim(),
            "Esc".cyan().bold(),
            ", or ".dim(),
            "q".cyan().bold(),
            " to close   ".into(),
            "Use ".dim(),
            "safedev ps --json".cyan(),
            " for scripts".dim(),
        ]))
        .alignment(Alignment::Center),
        rows[2],
    );
}

fn print_vm_list_table(items: &[VmListItem]) {
    if items.is_empty() {
        println!("No SafeDev workspaces found.");
        return;
    }
    println!(
        "{}  {}  {}  {}  {}  {}  {}  {}  {}",
        fit_cell("STATUS", 10),
        fit_cell("VM", 28),
        fit_cell("MODE", 8),
        fit_cell("CPU%", 7),
        fit_cell("RSS", 8),
        fit_cell("ALLOC", 10),
        fit_cell("DISK", 8),
        fit_cell("NETWORK", 10),
        "PROJECT"
    );
    for item in items {
        println!(
            "{}  {}  {}  {}  {}  {}  {}  {}  {}",
            fit_cell(&item.status, 10),
            fit_cell(&item.instance_name, 28),
            fit_cell(&item.mode, 8),
            fit_cell(&item.live_cpu_percent, 7),
            fit_cell(&item.live_memory, 8),
            fit_cell(&format!("{}/{}", item.cpus, item.memory), 10),
            fit_cell(&item.disk, 8),
            fit_cell(&item.network, 10),
            item.project_host_path
        );
    }
}

fn vm_list_json(items: &[VmListItem]) -> String {
    let entries = items
        .iter()
        .map(|item| {
            format!(
                concat!(
                    "  {{",
                    "\"instanceName\": {}, ",
                    "\"status\": {}, ",
                    "\"projectName\": {}, ",
                    "\"projectHostPath\": {}, ",
                    "\"mode\": {}, ",
                    "\"network\": {}, ",
                    "\"cpus\": {}, ",
                    "\"memory\": {}, ",
                    "\"disk\": {}, ",
                    "\"liveCpuPercent\": {}, ",
                    "\"liveMemory\": {}, ",
                    "\"workspaceId\": {}",
                    "}}"
                ),
                json_str(&item.instance_name),
                json_str(&item.status),
                json_str(&item.project_name),
                json_str(&item.project_host_path),
                json_str(&item.mode),
                json_str(&item.network),
                json_str(&item.cpus),
                json_str(&item.memory),
                json_str(&item.disk),
                json_str(&item.live_cpu_percent),
                json_str(&item.live_memory),
                json_str(&item.workspace_id)
            )
        })
        .collect::<Vec<_>>();
    format!("[\n{}\n]", entries.join(",\n"))
}

fn status_cell(status: &str, width: usize) -> Span<'static> {
    let value = fit_cell(status, width);
    match status.to_ascii_lowercase().as_str() {
        "running" => value.green(),
        "stopped" => value.dim(),
        "missing" | "unknown" => value.red(),
        _ => value.into(),
    }
}

fn fit_cell(value: &str, width: usize) -> String {
    let mut text = value.replace('\n', " ");
    if text.chars().count() > width {
        let keep = width.saturating_sub(2);
        text = text.chars().take(keep).collect::<String>();
        text.push_str("..");
    }
    format!("{text:<width$}")
}

fn build_policy(mode: &str) -> Policy {
    let mut policy = Policy {
        mode: mode.to_string(),
        host_home: false,
        project_writable: true,
        sandbox_home_persistent: false,
        cache_persistent: false,
        docker_socket: false,
        secrets_ambient: false,
        secrets_command_scoped: true,
        default_duration: "2h".to_string(),
        network_mode: "monitored".to_string(),
        block_metadata_ips: true,
        log_egress: true,
        blocked_metadata_ips: vec!["169.254.169.254".to_string(), "169.254.170.2".to_string()],
        network_allowlist: Vec::new(),
        install_scripts: "prompt".to_string(),
        block_new_versions_younger_than: "24h".to_string(),
        require_lockfile: "warn".to_string(),
        github_credential_mode: "scoped_ephemeral".to_string(),
        github_contents_permission: "read".to_string(),
        github_pull_requests_permission: "write".to_string(),
    };

    if mode == "locked" {
        policy.network_mode = "restricted".to_string();
        policy.install_scripts = "block".to_string();
        policy.require_lockfile = "error".to_string();
    }
    if mode == "trusted" {
        policy.sandbox_home_persistent = true;
        policy.cache_persistent = true;
        policy.network_mode = "broad_monitored".to_string();
    }
    policy
}

fn read_policy_for_state(state: &State) -> CliResult<Policy> {
    Ok(build_policy(&state.mode))
}

fn policy_json(policy: &Policy) -> String {
    format!(
        concat!(
            "{{\n",
            "  \"filesystem\": {{\n",
            "    \"host_home\": {},\n",
            "    \"project_writable\": {},\n",
            "    \"sandbox_home_persistent\": {},\n",
            "    \"docker_socket\": {},\n",
            "    \"cache_persistent\": {}\n",
            "  }},\n",
            "  \"secrets\": {{\n",
            "    \"ambient\": {},\n",
            "    \"command_scoped\": {},\n",
            "    \"default_duration\": {}\n",
            "  }},\n",
            "  \"network\": {{\n",
            "    \"mode\": {},\n",
            "    \"block_metadata_ips\": {},\n",
            "    \"log_egress\": {},\n",
            "    \"blocked_metadata_ips\": {},\n",
            "    \"allowlist\": {}\n",
            "  }},\n",
            "  \"packages\": {{\n",
            "    \"install_scripts\": {},\n",
            "    \"block_new_versions_younger_than\": {},\n",
            "    \"require_lockfile\": {}\n",
            "  }},\n",
            "  \"github\": {{\n",
            "    \"credential_mode\": {},\n",
            "    \"default_permissions\": {{\n",
            "      \"contents\": {},\n",
            "      \"pull_requests\": {}\n",
            "    }}\n",
            "  }},\n",
            "  \"mode\": {}\n",
            "}}\n"
        ),
        policy.host_home,
        policy.project_writable,
        policy.sandbox_home_persistent,
        policy.docker_socket,
        policy.cache_persistent,
        policy.secrets_ambient,
        policy.secrets_command_scoped,
        json_str(&policy.default_duration),
        json_str(&policy.network_mode),
        policy.block_metadata_ips,
        policy.log_egress,
        json_array(&policy.blocked_metadata_ips),
        json_array(&policy.network_allowlist),
        json_str(&policy.install_scripts),
        json_str(&policy.block_new_versions_younger_than),
        json_str(&policy.require_lockfile),
        json_str(&policy.github_credential_mode),
        json_str(&policy.github_contents_permission),
        json_str(&policy.github_pull_requests_permission),
        json_str(&policy.mode)
    )
}

fn broker_policy_json(policy: &Policy) -> String {
    format!(
        concat!(
            "{{\n",
            "  \"codex\": {{\n",
            "    \"mode\": \"sandbox_scoped\",\n",
            "    \"ambient\": false,\n",
            "    \"defaultDuration\": {},\n",
            "    \"hostCodexMount\": false,\n",
            "    \"sandboxConfigPath\": \"/home/dev/.codex\"\n",
            "  }},\n",
            "  \"github\": {{\n",
            "    \"credentialMode\": {},\n",
            "    \"defaultPermissions\": {{\n",
            "      \"contents\": {},\n",
            "      \"pull_requests\": {}\n",
            "    }},\n",
            "    \"ambient\": false,\n",
            "    \"commandScoped\": {},\n",
            "    \"defaultDuration\": {}\n",
            "  }}\n",
            "}}\n"
        ),
        json_str(&policy.default_duration),
        json_str(&policy.github_credential_mode),
        json_str(&policy.github_contents_permission),
        json_str(&policy.github_pull_requests_permission),
        policy.secrets_command_scoped,
        json_str(&policy.default_duration)
    )
}

fn is_package_install_command(argv: &[String]) -> bool {
    if argv.is_empty() {
        return false;
    }
    let subcommand = argv.get(1).map(String::as_str);
    match argv[0].as_str() {
        "npm" => matches!(subcommand, Some("install" | "i" | "ci" | "add")),
        "pnpm" => matches!(subcommand, Some("install" | "i" | "add")),
        "yarn" => subcommand.is_none() || matches!(subcommand, Some("install" | "add")),
        "bun" => matches!(subcommand, Some("install" | "add")),
        _ => false,
    }
}

fn enforce_install_script_policy(argv: &[String], policy: &Policy) -> Vec<String> {
    if !is_package_install_command(argv)
        || policy.install_scripts != "block"
        || argv.iter().any(|arg| arg == "--ignore-scripts")
    {
        return argv.to_vec();
    }
    let mut command = argv.to_vec();
    command.push("--ignore-scripts".to_string());
    command
}

fn enforce_lockfile_policy(state: &State, policy: &Policy) -> CliResult {
    if has_package_lockfile(&state.project_host_path) {
        return Ok(());
    }
    let message = "SafeDev warning: no package lockfile found; install may resolve unreviewed package versions.";
    if policy.require_lockfile == "error" {
        return Err(message.to_string());
    }
    if policy.require_lockfile == "warn" {
        eprintln!("{message}");
    }
    Ok(())
}

fn has_package_lockfile(project_path: &Path) -> bool {
    [
        "package-lock.json",
        "npm-shrinkwrap.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "bun.lock",
        "bun.lockb",
    ]
    .iter()
    .any(|file| project_path.join(file).exists())
}

fn safedev_home() -> PathBuf {
    env::var("SAFEDEV_HOME")
        .map(PathBuf::from)
        .or_else(|_| env::var("HOME").map(|home| PathBuf::from(home).join(".safedev")))
        .unwrap_or_else(|_| PathBuf::from(".safedev"))
}

fn resolve_project(project: Option<&str>) -> CliResult<PathBuf> {
    let project = project
        .map(PathBuf::from)
        .unwrap_or(env::current_dir().map_err(|error| error.to_string())?);
    if !project.exists() {
        return Err(format!(
            "Project path does not exist: {}",
            project.display()
        ));
    }
    if !project.is_dir() {
        return Err(format!(
            "Project path is not a directory: {}",
            project.display()
        ));
    }
    fs::canonicalize(&project)
        .map_err(|error| format!("failed to resolve {}: {error}", project.display()))
}

fn workspace_id(project_path: &Path) -> String {
    format!(
        "{}-{}",
        safe_name(
            project_path
                .file_name()
                .and_then(OsStr::to_str)
                .unwrap_or("project")
        ),
        &stable_hash_hex(&project_path.to_string_lossy())[0..12]
    )
}

fn workspace_paths(project_path: &Path) -> StatePaths {
    let id = workspace_id(project_path);
    let root = safedev_home().join("workspaces").join(id);
    StatePaths {
        state_json: root.join("state.json"),
        state_env: root.join("state.env"),
        policy_file: root.join("policy.json"),
        lima_file: root.join("lima.yaml"),
        devcontainer_file: root.join("devcontainer.json"),
        project_profile_file: root.join("project-profile.json"),
        codex_dir: root.join("codex"),
        credentials_dir: root.join("credentials"),
        snapshots_dir: root.join("snapshots"),
        snapshots_file: root.join("snapshots.tsv"),
        rollback_backups_dir: root.join("rollback-backups"),
        history_file: root.join("history.jsonl"),
        last_inspect_file: root.join("last-inspect.json"),
        last_inspect_env: root.join("last-inspect.env"),
        root,
    }
}

fn init_state(
    project_path: PathBuf,
    mode: &str,
    devcontainer: Option<Devcontainer>,
    project_profile: ProjectProfile,
) -> CliResult<State> {
    let paths = workspace_paths(&project_path);
    ensure_dir(&paths.root)?;
    ensure_dir(&paths.snapshots_dir)?;
    ensure_dir(&paths.codex_dir)?;
    ensure_dir(&paths.credentials_dir)?;
    let project_name = safe_name(
        project_path
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or("project"),
    );
    Ok(State {
        id: workspace_id(&project_path),
        mode: mode.to_string(),
        created_at: now_stamp(),
        status: "created".to_string(),
        instance_name: format!(
            "safedev-{}-{}",
            project_name,
            &stable_hash_hex(&project_path.to_string_lossy())[0..8]
        ),
        project_name: project_name.clone(),
        project_host_path: project_path,
        project_vm_path: format!("/workspaces/{project_name}"),
        paths,
        policy_summary_network: build_policy(mode).network_mode,
        policy_summary_install_scripts: build_policy(mode).install_scripts,
        devcontainer,
        project_profile,
        snapshots: Vec::new(),
    })
}

fn load_state(project: Option<&str>) -> CliResult<State> {
    let project_path = resolve_project(project)?;
    let paths = workspace_paths(&project_path);
    if !paths.state_env.exists() {
        return Err(format!(
            "No SafeDev workspace found for {}. Run \"safedev up\" first.",
            project_path.display()
        ));
    }
    let values = read_kv(&paths.state_env)?;
    let mode = required_kv(&values, "mode")?;
    let policy = build_policy(&mode);
    Ok(State {
        id: required_kv(&values, "id")?,
        mode,
        created_at: required_kv(&values, "created_at")?,
        status: required_kv(&values, "status")?,
        instance_name: required_kv(&values, "instance_name")?,
        project_name: required_kv(&values, "project_name")?,
        project_host_path: project_path,
        project_vm_path: required_kv(&values, "project_vm_path")?,
        paths: paths.clone(),
        policy_summary_network: policy.network_mode,
        policy_summary_install_scripts: policy.install_scripts,
        devcontainer: load_devcontainer_from_state(&paths.devcontainer_file)?,
        project_profile: load_project_profile_from_state(&paths.project_profile_file)?,
        snapshots: load_snapshots(&paths.snapshots_file)?,
    })
}

fn save_state(state: &State) -> CliResult {
    ensure_dir(&state.paths.root)?;
    ensure_dir(&state.paths.codex_dir)?;
    ensure_dir(&state.paths.credentials_dir)?;
    ensure_dir(&state.paths.snapshots_dir)?;
    let mut env_text = String::new();
    for (key, value) in [
        ("id", state.id.as_str()),
        ("mode", state.mode.as_str()),
        ("created_at", state.created_at.as_str()),
        ("status", state.status.as_str()),
        ("instance_name", state.instance_name.as_str()),
        ("project_name", state.project_name.as_str()),
        (
            "project_host_path",
            &state.project_host_path.to_string_lossy(),
        ),
        ("project_vm_path", state.project_vm_path.as_str()),
    ] {
        env_text.push_str(key);
        env_text.push('=');
        env_text.push_str(value);
        env_text.push('\n');
    }
    write_text(&state.paths.state_env, &env_text)?;
    save_snapshots(&state.paths.snapshots_file, &state.snapshots)?;
    write_text(&state.paths.state_json, &state_json(state))?;
    Ok(())
}

fn load_vm_list_items() -> CliResult<Vec<VmListItem>> {
    let lima_infos = lima_instance_infos().unwrap_or_default();
    let usage_by_pid = process_usage_by_pid(lima_infos.values().filter_map(|info| {
        info.driver_pid
            .as_ref()
            .filter(|pid| pid.as_str() != "0")
            .cloned()
    }));
    let workspace_root = safedev_home().join("workspaces");
    let mut items = Vec::new();
    let mut seen = HashSet::new();

    if workspace_root.exists() {
        for entry in fs::read_dir(&workspace_root)
            .map_err(|error| format!("failed to read {}: {error}", workspace_root.display()))?
        {
            let entry = entry.map_err(|error| {
                format!(
                    "failed to read entry in {}: {error}",
                    workspace_root.display()
                )
            })?;
            let state_env = entry.path().join("state.env");
            if !state_env.is_file() {
                continue;
            }
            let values = read_kv(&state_env)?;
            let instance_name = required_kv(&values, "instance_name")?;
            let mode = required_kv(&values, "mode")?;
            let policy = build_policy(&mode);
            let info = lima_infos.get(&instance_name);
            let usage = info
                .and_then(|info| info.driver_pid.as_ref())
                .and_then(|pid| usage_by_pid.get(pid));
            seen.insert(instance_name.clone());
            items.push(VmListItem {
                instance_name,
                status: info
                    .map(|info| info.status.clone())
                    .unwrap_or_else(|| "Missing".to_string()),
                project_name: required_kv(&values, "project_name")?,
                project_host_path: required_kv(&values, "project_host_path")?,
                mode,
                network: policy.network_mode,
                cpus: info
                    .map(|info| info.cpus.clone())
                    .unwrap_or_else(|| "-".to_string()),
                memory: info
                    .map(|info| info.memory.clone())
                    .unwrap_or_else(|| "-".to_string()),
                disk: info
                    .map(|info| info.disk.clone())
                    .unwrap_or_else(|| "-".to_string()),
                live_cpu_percent: usage
                    .map(|usage| usage.cpu_percent.clone())
                    .unwrap_or_else(|| "-".to_string()),
                live_memory: usage
                    .map(|usage| usage.rss.clone())
                    .unwrap_or_else(|| "-".to_string()),
                workspace_id: required_kv(&values, "id")?,
            });
        }
    }

    for (instance_name, info) in lima_infos {
        if instance_name.starts_with("safedev-") && !seen.contains(&instance_name) {
            let usage = info
                .driver_pid
                .as_ref()
                .and_then(|pid| usage_by_pid.get(pid));
            items.push(VmListItem {
                instance_name,
                status: info.status,
                project_name: "-".to_string(),
                project_host_path: "unmanaged SafeDev Lima instance".to_string(),
                mode: "-".to_string(),
                network: "-".to_string(),
                cpus: info.cpus,
                memory: info.memory,
                disk: info.disk,
                live_cpu_percent: usage
                    .map(|usage| usage.cpu_percent.clone())
                    .unwrap_or_else(|| "-".to_string()),
                live_memory: usage
                    .map(|usage| usage.rss.clone())
                    .unwrap_or_else(|| "-".to_string()),
                workspace_id: "-".to_string(),
            });
        }
    }

    items.sort_by(|left, right| {
        status_rank(&left.status)
            .cmp(&status_rank(&right.status))
            .then_with(|| left.project_name.cmp(&right.project_name))
            .then_with(|| left.instance_name.cmp(&right.instance_name))
    });
    Ok(items)
}

fn status_rank(status: &str) -> u8 {
    match status.to_ascii_lowercase().as_str() {
        "running" => 0,
        "stopped" => 1,
        "missing" => 2,
        _ => 3,
    }
}

fn lima_instance_infos() -> CliResult<HashMap<String, LimaInstanceInfo>> {
    let output = run_limactl(&[
        "list".to_string(),
        "--format".to_string(),
        "{{.Name}}\t{{.Status}}\t{{.CPUs}}\t{{.Memory}}\t{{.Disk}}\t{{.DriverPID}}".to_string(),
    ])?;
    if !output.status.success() {
        return Err(output_text(&output));
    }
    let mut infos = HashMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if line.starts_with("NAME") {
            continue;
        }
        let parts = line.split('\t').collect::<Vec<_>>();
        if parts.len() >= 5 {
            infos.insert(
                parts[0].to_string(),
                LimaInstanceInfo {
                    status: parts[1].to_string(),
                    cpus: parts[2].to_string(),
                    memory: format_resource_value(parts[3]),
                    disk: format_resource_value(parts[4]),
                    driver_pid: parts.get(5).map(|pid| pid.to_string()),
                },
            );
            continue;
        }

        let parts = line.split_whitespace().collect::<Vec<_>>();
        if parts.len() >= 5 {
            infos.insert(
                parts[0].to_string(),
                LimaInstanceInfo {
                    status: parts[1].to_string(),
                    cpus: parts[2].to_string(),
                    memory: format_resource_value(parts[3]),
                    disk: format_resource_value(parts[4]),
                    driver_pid: parts.get(5).map(|pid| pid.to_string()),
                },
            );
        }
    }
    Ok(infos)
}

fn process_usage_by_pid<I>(pids: I) -> HashMap<String, ProcessUsage>
where
    I: IntoIterator<Item = String>,
{
    let mut pid_list = pids.into_iter().collect::<Vec<_>>();
    pid_list.sort();
    pid_list.dedup();
    if pid_list.is_empty() {
        return HashMap::new();
    }

    let output = Command::new("ps")
        .args(["-o", "pid=", "-o", "%cpu=", "-o", "rss=", "-p"])
        .arg(pid_list.join(","))
        .output();
    let Ok(output) = output else {
        return HashMap::new();
    };
    if !output.status.success() {
        return HashMap::new();
    }

    let mut usage = HashMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let parts = line.split_whitespace().collect::<Vec<_>>();
        if parts.len() < 3 {
            continue;
        }
        let cpu = parts[1].trim_end_matches('%');
        let rss = parts[2]
            .parse::<u64>()
            .map(|kib| format_bytes(kib.saturating_mul(1024)))
            .unwrap_or_else(|_| "-".to_string());
        usage.insert(
            parts[0].to_string(),
            ProcessUsage {
                cpu_percent: format!("{cpu}%"),
                rss,
            },
        );
    }
    usage
}

fn format_resource_value(value: &str) -> String {
    if value.ends_with("B") || value.ends_with("iB") || value == "-" {
        return value.to_string();
    }
    match value.parse::<u64>() {
        Ok(bytes) => format_bytes(bytes),
        Err(_) => value.to_string(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    if bytes >= 1024 * 1024 * 1024 {
        let gib = bytes as f64 / GIB;
        if (gib.fract()).abs() < f64::EPSILON {
            format!("{}GiB", gib as u64)
        } else {
            format!("{gib:.1}GiB")
        }
    } else if bytes >= 1024 * 1024 {
        let mib = bytes as f64 / MIB;
        if (mib.fract()).abs() < f64::EPSILON {
            format!("{}MiB", mib as u64)
        } else {
            format!("{mib:.1}MiB")
        }
    } else {
        format!("{bytes}B")
    }
}

fn state_json(state: &State) -> String {
    let devcontainer = state
        .devcontainer
        .as_ref()
        .map(devcontainer_json)
        .unwrap_or_else(|| "null\n".to_string());
    format!(
        concat!(
            "{{\n",
            "  \"version\": 1,\n",
            "  \"id\": {},\n",
            "  \"mode\": {},\n",
            "  \"createdAt\": {},\n",
            "  \"updatedAt\": {},\n",
            "  \"status\": {},\n",
            "  \"instanceName\": {},\n",
            "  \"project\": {{\n",
            "    \"name\": {},\n",
            "    \"hostPath\": {},\n",
            "    \"vmPath\": {}\n",
            "  }},\n",
            "  \"paths\": {},\n",
            "  \"policySummary\": {{\n",
            "    \"host_home\": false,\n",
            "    \"docker_socket\": false,\n",
            "    \"network\": {},\n",
            "    \"install_scripts\": {}\n",
            "  }},\n",
            "  \"devcontainer\": {},\n",
            "  \"projectProfile\": {},\n",
            "  \"snapshots\": {}\n",
            "}}\n"
        ),
        json_str(&state.id),
        json_str(&state.mode),
        json_str(&state.created_at),
        json_str(&now_stamp()),
        json_str(&state.status),
        json_str(&state.instance_name),
        json_str(&state.project_name),
        json_str(&state.project_host_path.to_string_lossy()),
        json_str(&state.project_vm_path),
        paths_json(&state.paths),
        json_str(&state.policy_summary_network),
        json_str(&state.policy_summary_install_scripts),
        devcontainer.trim_end(),
        project_profile_json(&state.project_profile).trim_end(),
        snapshots_json(&state.snapshots)
    )
}

fn paths_json(paths: &StatePaths) -> String {
    format!(
        concat!(
            "{{\n",
            "    \"root\": {},\n",
            "    \"stateFile\": {},\n",
            "    \"policyFile\": {},\n",
            "    \"limaFile\": {},\n",
            "    \"devcontainerFile\": {},\n",
            "    \"projectProfileFile\": {},\n",
            "    \"codexDir\": {},\n",
            "    \"credentialsDir\": {},\n",
            "    \"snapshotsDir\": {},\n",
            "    \"rollbackBackupsDir\": {},\n",
            "    \"historyFile\": {},\n",
            "    \"lastInspectFile\": {}\n",
            "  }}"
        ),
        json_str(&paths.root.to_string_lossy()),
        json_str(&paths.state_json.to_string_lossy()),
        json_str(&paths.policy_file.to_string_lossy()),
        json_str(&paths.lima_file.to_string_lossy()),
        json_str(&paths.devcontainer_file.to_string_lossy()),
        json_str(&paths.project_profile_file.to_string_lossy()),
        json_str(&paths.codex_dir.to_string_lossy()),
        json_str(&paths.credentials_dir.to_string_lossy()),
        json_str(&paths.snapshots_dir.to_string_lossy()),
        json_str(&paths.rollback_backups_dir.to_string_lossy()),
        json_str(&paths.history_file.to_string_lossy()),
        json_str(&paths.last_inspect_file.to_string_lossy())
    )
}

fn snapshots_json(snapshots: &[Snapshot]) -> String {
    let values: Vec<String> = snapshots
        .iter()
        .map(|snapshot| {
            format!(
                "{{\"label\":{},\"reason\":{},\"createdAt\":{},\"projectPath\":{}}}",
                json_str(&snapshot.label),
                json_str(&snapshot.reason),
                json_str(&snapshot.created_at),
                json_str(&snapshot.project_path)
            )
        })
        .collect();
    format!("[{}]", values.join(","))
}

fn load_snapshots(path: &Path) -> CliResult<Vec<Snapshot>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let mut snapshots = Vec::new();
    for line in text.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() == 4 {
            snapshots.push(Snapshot {
                label: parts[0].to_string(),
                reason: parts[1].to_string(),
                created_at: parts[2].to_string(),
                project_path: parts[3].to_string(),
            });
        }
    }
    Ok(snapshots)
}

fn save_snapshots(path: &Path, snapshots: &[Snapshot]) -> CliResult {
    let mut text = String::new();
    for snapshot in snapshots {
        text.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            snapshot.label, snapshot.reason, snapshot.created_at, snapshot.project_path
        ));
    }
    write_text(path, &text)
}

fn load_devcontainer(project_path: &Path) -> CliResult<Option<Devcontainer>> {
    let file = project_path.join(".devcontainer").join("devcontainer.json");
    if !file.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&file)
        .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
    let stripped = strip_json_comments(&text);
    Ok(Some(Devcontainer {
        path: file.to_string_lossy().to_string(),
        name: extract_json_string(&stripped, "name"),
        image: extract_json_string(&stripped, "image"),
        docker_file: extract_json_string(&stripped, "dockerFile")
            .or_else(|| extract_json_string(&stripped, "dockerfile")),
        remote_user: extract_json_string(&stripped, "remoteUser"),
        post_create_command: extract_json_string(&stripped, "postCreateCommand"),
    }))
}

fn load_devcontainer_from_state(path: &Path) -> CliResult<Option<Devcontainer>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    Ok(Some(Devcontainer {
        path: extract_json_string(&text, "path").unwrap_or_default(),
        name: extract_json_string(&text, "name"),
        image: extract_json_string(&text, "image"),
        docker_file: extract_json_string(&text, "dockerFile"),
        remote_user: extract_json_string(&text, "remoteUser"),
        post_create_command: extract_json_string(&text, "postCreateCommand"),
    }))
}

fn devcontainer_json(devcontainer: &Devcontainer) -> String {
    format!(
        concat!(
            "{{\n",
            "  \"path\": {},\n",
            "  \"name\": {},\n",
            "  \"image\": {},\n",
            "  \"dockerFile\": {},\n",
            "  \"remoteUser\": {},\n",
            "  \"postCreateCommand\": {},\n",
            "  \"features\": [],\n",
            "  \"config\": {{}}\n",
            "}}\n"
        ),
        json_str(&devcontainer.path),
        json_option(devcontainer.name.as_deref()),
        json_option(devcontainer.image.as_deref()),
        json_option(devcontainer.docker_file.as_deref()),
        json_option(devcontainer.remote_user.as_deref()),
        json_option(devcontainer.post_create_command.as_deref())
    )
}

fn detect_project_profile(project_path: &Path) -> CliResult<ProjectProfile> {
    let mut profile = ProjectProfile {
        javascript: false,
        python: false,
        rust: false,
        tilt: false,
        package_managers: Vec::new(),
        manifests: Vec::new(),
    };
    scan_project_manifests(project_path, project_path, &mut profile)?;
    profile.package_managers.sort();
    profile.package_managers.dedup();
    profile
        .manifests
        .sort_by(|left, right| left.path.cmp(&right.path));
    Ok(profile)
}

fn scan_project_manifests(root: &Path, current: &Path, profile: &mut ProjectProfile) -> CliResult {
    for entry in fs::read_dir(current)
        .map_err(|error| format!("failed to read {}: {error}", current.display()))?
    {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        let file_name = entry.file_name().to_string_lossy().to_string();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("failed to stat {}: {error}", path.display()))?;
        if metadata.is_dir() {
            if should_skip_scan_dir(&file_name) {
                continue;
            }
            scan_project_manifests(root, &path, profile)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        match file_name.as_str() {
            "package.json" => {
                profile.javascript = true;
                let package_manager = fs::read_to_string(&path)
                    .ok()
                    .and_then(|text| extract_json_string(&text, "packageManager"));
                if let Some(package_manager) = &package_manager {
                    push_unique(&mut profile.package_managers, package_manager.clone());
                }
                profile.manifests.push(ProjectManifest {
                    ecosystem: "javascript".to_string(),
                    path: rel,
                    package_manager,
                });
            }
            "pnpm-lock.yaml"
            | "yarn.lock"
            | "package-lock.json"
            | "npm-shrinkwrap.json"
            | "bun.lock"
            | "bun.lockb" => {
                profile.javascript = true;
                profile.manifests.push(ProjectManifest {
                    ecosystem: "javascript-lockfile".to_string(),
                    path: rel,
                    package_manager: None,
                });
            }
            "Cargo.toml" => {
                profile.rust = true;
                profile.manifests.push(ProjectManifest {
                    ecosystem: "rust".to_string(),
                    path: rel,
                    package_manager: Some("cargo".to_string()),
                });
            }
            "pyproject.toml" | "requirements.txt" | "setup.py" | "setup.cfg" | "Pipfile"
            | "poetry.lock" | "uv.lock" => {
                profile.python = true;
                profile.manifests.push(ProjectManifest {
                    ecosystem: "python".to_string(),
                    path: rel,
                    package_manager: python_package_manager(&file_name),
                });
            }
            "Tiltfile" => {
                profile.tilt = true;
                profile.manifests.push(ProjectManifest {
                    ecosystem: "tilt".to_string(),
                    path: rel,
                    package_manager: Some("tilt".to_string()),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn should_skip_scan_dir(name: &str) -> bool {
    matches!(
        name,
        ".git"
            | ".hg"
            | ".svn"
            | "node_modules"
            | "target"
            | ".venv"
            | "venv"
            | "__pycache__"
            | ".mypy_cache"
            | ".pytest_cache"
            | ".ruff_cache"
            | ".next"
            | "dist"
            | "build"
            | ".safedev"
    )
}

fn python_package_manager(file_name: &str) -> Option<String> {
    match file_name {
        "pyproject.toml" => Some("python".to_string()),
        "requirements.txt" => Some("pip".to_string()),
        "Pipfile" => Some("pipenv".to_string()),
        "poetry.lock" => Some("poetry".to_string()),
        "uv.lock" => Some("uv".to_string()),
        _ => None,
    }
}

fn load_project_profile_from_state(path: &Path) -> CliResult<ProjectProfile> {
    if !path.exists() {
        return Ok(ProjectProfile {
            javascript: false,
            python: false,
            rust: false,
            tilt: false,
            package_managers: Vec::new(),
            manifests: Vec::new(),
        });
    }
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    Ok(ProjectProfile {
        javascript: extract_json_bool(&text, "javascript").unwrap_or(false),
        python: extract_json_bool(&text, "python").unwrap_or(false),
        rust: extract_json_bool(&text, "rust").unwrap_or(false),
        tilt: extract_json_bool(&text, "tilt").unwrap_or(false),
        package_managers: extract_json_string_array(&text, "packageManagers"),
        manifests: Vec::new(),
    })
}

fn project_profile_json(profile: &ProjectProfile) -> String {
    let manifests = profile
        .manifests
        .iter()
        .map(|manifest| {
            format!(
                "{{\"ecosystem\":{},\"path\":{},\"packageManager\":{}}}",
                json_str(&manifest.ecosystem),
                json_str(&manifest.path),
                json_option(manifest.package_manager.as_deref())
            )
        })
        .collect::<Vec<String>>()
        .join(", ");
    format!(
        "{{\n  \"toolchains\": {{\n    \"javascript\": {},\n    \"python\": {},\n    \"rust\": {},\n    \"tilt\": {}\n  }},\n  \"packageManagers\": {},\n  \"manifests\": [{}]\n}}\n",
        profile.javascript,
        profile.python,
        profile.rust,
        profile.tilt,
        json_array(&profile.package_managers),
        manifests
    )
}

fn strip_json_comments(input: &str) -> String {
    let mut output = String::new();
    let mut chars = input.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    while let Some(current) = chars.next() {
        if in_string {
            output.push(current);
            if escaped {
                escaped = false;
            } else if current == '\\' {
                escaped = true;
            } else if current == '"' {
                in_string = false;
            }
            continue;
        }
        if current == '"' {
            in_string = true;
            output.push(current);
            continue;
        }
        if current == '/' && chars.peek() == Some(&'/') {
            for next in chars.by_ref() {
                if next == '\n' {
                    output.push('\n');
                    break;
                }
            }
            continue;
        }
        if current == '/' && chars.peek() == Some(&'*') {
            chars.next();
            let mut previous = '\0';
            for next in chars.by_ref() {
                if previous == '*' && next == '/' {
                    break;
                }
                previous = next;
            }
            continue;
        }
        output.push(current);
    }
    output
}

fn extract_json_string(input: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\"");
    let start = input.find(&needle)?;
    let after_key = &input[start + needle.len()..];
    let colon = after_key.find(':')?;
    let after_colon = after_key[colon + 1..].trim_start();
    if after_colon.starts_with("null") {
        return None;
    }
    let mut chars = after_colon.chars();
    if chars.next()? != '"' {
        return None;
    }
    let mut value = String::new();
    let mut escaped = false;
    for current in chars {
        if escaped {
            let resolved = match current {
                '"' => '"',
                '\\' => '\\',
                '/' => '/',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            };
            value.push(resolved);
            escaped = false;
        } else if current == '\\' {
            escaped = true;
        } else if current == '"' {
            return Some(value);
        } else {
            value.push(current);
        }
    }
    None
}

fn extract_json_bool(input: &str, key: &str) -> Option<bool> {
    let needle = format!("\"{key}\"");
    let start = input.find(&needle)?;
    let after_key = &input[start + needle.len()..];
    let colon = after_key.find(':')?;
    let after_colon = after_key[colon + 1..].trim_start();
    if after_colon.starts_with("true") {
        Some(true)
    } else if after_colon.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn extract_json_string_array(input: &str, key: &str) -> Vec<String> {
    let needle = format!("\"{key}\"");
    let Some(start) = input.find(&needle) else {
        return Vec::new();
    };
    let after_key = &input[start + needle.len()..];
    let Some(colon) = after_key.find(':') else {
        return Vec::new();
    };
    let after_colon = after_key[colon + 1..].trim_start();
    let Some(open) = after_colon.find('[') else {
        return Vec::new();
    };
    let after_open = &after_colon[open + 1..];
    let Some(close) = after_open.find(']') else {
        return Vec::new();
    };
    let mut values = Vec::new();
    let mut cursor = &after_open[..close];
    while let Some(start_quote) = cursor.find('"') {
        cursor = &cursor[start_quote + 1..];
        let mut value = String::new();
        let mut escaped = false;
        let mut end_index = None;
        for (index, current) in cursor.char_indices() {
            if escaped {
                value.push(current);
                escaped = false;
            } else if current == '\\' {
                escaped = true;
            } else if current == '"' {
                end_index = Some(index + 1);
                break;
            } else {
                value.push(current);
            }
        }
        values.push(value);
        if let Some(end_index) = end_index {
            cursor = &cursor[end_index..];
        } else {
            break;
        }
    }
    values
}

fn render_lima_config(state: &State, policy: &Policy) -> String {
    let arch = if env::consts::ARCH == "aarch64" || env::consts::ARCH == "arm64" {
        "aarch64"
    } else {
        "x86_64"
    };
    let image_arch = if arch == "aarch64" { "arm64" } else { "amd64" };
    let metadata_blocks = policy
        .blocked_metadata_ips
        .iter()
        .map(|ip| format!("      ip route add blackhole {ip}/32 2>/dev/null || true"))
        .collect::<Vec<String>>()
        .join("\n");
    let apt_packages = provision_apt_packages(&state.project_profile).join(" ");
    let toolchain_bootstrap = render_toolchain_bootstrap(&state.project_profile);
    format!(
        concat!(
            "# Generated by SafeDev. Host home, host secrets, browser profiles, Docker socket, and ~/.codex are intentionally not mounted.\n",
            "vmType: \"vz\"\n",
            "os: \"Linux\"\n",
            "arch: \"{arch}\"\n",
            "mountType: \"virtiofs\"\n\n",
            "images:\n",
            "  - location: \"https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-{image_arch}.img\"\n",
            "    arch: \"{arch}\"\n\n",
            "mounts:\n",
            "  - location: {project_path}\n",
            "    mountPoint: {vm_path}\n",
            "    writable: true\n\n",
            "portForwards:\n",
            "  - guestPort: 3000\n",
            "    hostPort: 3000\n",
            "  - guestPort: 3001\n",
            "    hostPort: 3001\n",
            "  - guestPort: 5173\n",
            "    hostPort: 5173\n",
            "  - guestPort: 8000\n",
            "    hostPort: 8000\n",
            "  - guestPort: 8080\n",
            "    hostPort: 8080\n\n",
            "containerd:\n",
            "  system: false\n",
            "  user: false\n\n",
            "provision:\n",
            "  - mode: system\n",
            "    script: |\n",
            "      set -eu\n",
            "      id dev >/dev/null 2>&1 || useradd -m -s /bin/bash dev\n",
            "      install -d -m 0755 /workspaces /etc/safedev /var/log/safedev\n",
            "      install -d -o dev -g dev -m 0700 /home/dev /home/dev/.codex /home/dev/.safedev\n",
            "      printf '%s\\n' {profile_json} >/etc/safedev/project-profile.json\n",
            "      if command -v apt-get >/dev/null 2>&1; then\n",
            "        apt-get update\n",
            "        DEBIAN_FRONTEND=noninteractive apt-get install -y {apt_packages}\n",
            "      fi\n",
            "{toolchain_bootstrap}",
            "      cat >/usr/local/bin/safedev-egress-proxy.js <<'SAFEDEV_PROXY'\n",
            "{proxy}\n",
            "      SAFEDEV_PROXY\n",
            "      chmod +x /usr/local/bin/safedev-egress-proxy.js\n",
            "      cat >/etc/systemd/system/safedev-egress-proxy.service <<'SAFEDEV_SERVICE'\n",
            "      [Unit]\n",
            "      Description=SafeDev egress monitor proxy\n",
            "      After=network-online.target\n\n",
            "      [Service]\n",
            "      Environment=SAFEDEV_NETWORK_MODE={network_mode}\n",
            "      Environment=SAFEDEV_EGRESS_LOG=/var/log/safedev/egress.log\n",
            "      Environment=SAFEDEV_BLOCK_METADATA_IPS={metadata_ips}\n",
            "      Environment=SAFEDEV_NETWORK_ALLOWLIST={allowlist}\n",
            "      ExecStart=/usr/bin/node /usr/local/bin/safedev-egress-proxy.js\n",
            "      Restart=always\n\n",
            "      [Install]\n",
            "      WantedBy=multi-user.target\n",
            "      SAFEDEV_SERVICE\n",
            "      systemctl daemon-reload 2>/dev/null || true\n",
            "      systemctl enable --now safedev-egress-proxy.service 2>/dev/null || true\n",
            "      cat >/etc/profile.d/safedev-network.sh <<'SAFEDEV_PROFILE'\n",
            "      export HTTP_PROXY=http://127.0.0.1:18080\n",
            "      export HTTPS_PROXY=http://127.0.0.1:18080\n",
            "      export http_proxy=http://127.0.0.1:18080\n",
            "      export https_proxy=http://127.0.0.1:18080\n",
            "      export NO_PROXY=localhost,127.0.0.1,::1\n",
            "      export no_proxy=localhost,127.0.0.1,::1\n",
            "      SAFEDEV_PROFILE\n",
            "      cat >/etc/safedev/policy.json <<'SAFEDEV_POLICY'\n",
            "{policy_json}\n",
            "      SAFEDEV_POLICY\n",
            "{metadata_blocks}\n",
            "      printf '%s\\n' 'SafeDev protects your Mac. Codex works inside SafeDev.' >/etc/safedev/message\n"
        ),
        arch = arch,
        image_arch = image_arch,
        project_path = json_str(&state.project_host_path.to_string_lossy()),
        vm_path = json_str(&state.project_vm_path),
        profile_json = shell_quote(&compact_json(&project_profile_json(&state.project_profile))),
        apt_packages = apt_packages,
        toolchain_bootstrap = toolchain_bootstrap,
        proxy = indent_block(NETWORK_PROXY_JS, 6),
        network_mode = policy.network_mode,
        metadata_ips = policy.blocked_metadata_ips.join(","),
        allowlist = policy.network_allowlist.join(","),
        policy_json = indent_block(&policy_json(policy), 6),
        metadata_blocks = metadata_blocks
    )
}

fn provision_apt_packages(profile: &ProjectProfile) -> Vec<&'static str> {
    let mut packages = vec![
        "ca-certificates",
        "curl",
        "git",
        "build-essential",
        "pkg-config",
    ];
    if profile.javascript {
        packages.extend(["nodejs", "npm"]);
    }
    if profile.python {
        packages.extend(["python3", "python3-venv", "python3-pip", "pipx"]);
    }
    if profile.rust {
        packages.extend(["cargo", "rustc"]);
    }
    packages.sort();
    packages.dedup();
    packages
}

fn render_toolchain_bootstrap(profile: &ProjectProfile) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "      printf '%s\\n' {}",
        shell_quote(&format!(
            "Detected toolchains: js={}, python={}, rust={}, tilt={}",
            profile.javascript, profile.python, profile.rust, profile.tilt
        ))
    ));
    if profile.javascript {
        lines.push("      if command -v npm >/dev/null 2>&1; then".to_string());
        lines.push("        npm install -g corepack pnpm yarn".to_string());
        lines.push("        corepack enable || true".to_string());
        for package_manager in &profile.package_managers {
            if package_manager.starts_with("pnpm@")
                || package_manager.starts_with("yarn@")
                || package_manager.starts_with("npm@")
            {
                lines.push(format!(
                    "        corepack prepare {} --activate || true",
                    shell_quote(package_manager)
                ));
            }
        }
        lines.push("      fi".to_string());
    }
    if profile.python {
        lines.push("      if command -v python3 >/dev/null 2>&1; then".to_string());
        lines.push(
            "        python3 -m pip install --break-system-packages --upgrade uv virtualenv || true"
                .to_string(),
        );
        lines.push("      fi".to_string());
    }
    if profile.rust {
        lines.push("      if command -v cargo >/dev/null 2>&1; then cargo --version >/etc/safedev/cargo-version 2>/dev/null || true; fi".to_string());
    }
    if profile.tilt {
        lines.push("      if ! command -v tilt >/dev/null 2>&1; then".to_string());
        lines.push("        curl -fsSL https://raw.githubusercontent.com/tilt-dev/tilt/master/scripts/install.sh | bash".to_string());
        lines.push("      fi".to_string());
    }
    format!("{}\n", lines.join("\n"))
}

fn ensure_vm_toolchains_with_progress(state: &State, progress: &mut UpProgress) -> CliResult {
    let script = render_runtime_toolchain_ensure(&state.project_profile);
    let output = run_shell_in_instance_with_progress(state, &script, progress)?;
    if !output.status.success() {
        progress.fail(4, "Toolchain verification failed");
        progress.finish();
        print_output(&output);
        process::exit(output.status.code().unwrap_or(1));
    }
    Ok(())
}

fn render_runtime_toolchain_ensure(profile: &ProjectProfile) -> String {
    let mut packages = Vec::new();
    if profile.javascript {
        packages.extend(["nodejs", "npm"]);
    }
    if profile.python {
        packages.extend(["python3", "python3-venv", "python3-pip", "pipx"]);
    }
    if profile.rust {
        packages.extend(["cargo", "rustc"]);
    }
    packages.sort();
    packages.dedup();

    let mut lines = vec![
        "set -eu".to_string(),
        "sudo install -d -m 0755 /etc/safedev".to_string(),
        format!(
            "printf '%s\\n' {} | sudo tee /etc/safedev/project-profile.json >/dev/null",
            shell_quote(&compact_json(&project_profile_json(profile)))
        ),
    ];

    if !packages.is_empty() {
        lines.push("missing_packages=''".to_string());
        if profile.javascript {
            lines.push("if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then missing_packages=\"$missing_packages nodejs npm\"; fi".to_string());
        }
        if profile.python {
            lines.push("if ! command -v python3 >/dev/null 2>&1 || ! command -v pipx >/dev/null 2>&1; then missing_packages=\"$missing_packages python3 python3-venv python3-pip pipx\"; fi".to_string());
        }
        if profile.rust {
            lines.push("if ! command -v cargo >/dev/null 2>&1 || ! command -v rustc >/dev/null 2>&1; then missing_packages=\"$missing_packages cargo rustc\"; fi".to_string());
        }
        lines.push("if [ -n \"$missing_packages\" ]; then sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $missing_packages; fi".to_string());
    }

    if profile.javascript {
        lines.push("if ! command -v pnpm >/dev/null 2>&1 || ! command -v yarn >/dev/null 2>&1; then sudo npm install -g corepack pnpm yarn; fi".to_string());
        lines.push(
            "if command -v corepack >/dev/null 2>&1; then sudo corepack enable || true; fi"
                .to_string(),
        );
        for package_manager in &profile.package_managers {
            if package_manager.starts_with("pnpm@")
                || package_manager.starts_with("yarn@")
                || package_manager.starts_with("npm@")
            {
                lines.push(format!(
                    "if command -v corepack >/dev/null 2>&1; then sudo corepack prepare {} --activate || true; fi",
                    shell_quote(package_manager)
                ));
            }
        }
    }
    if profile.python {
        lines.push("if command -v python3 >/dev/null 2>&1 && ! command -v uv >/dev/null 2>&1; then sudo python3 -m pip install --break-system-packages --upgrade uv virtualenv || true; fi".to_string());
    }
    if profile.tilt {
        lines.push("if ! command -v tilt >/dev/null 2>&1; then curl -fsSL https://raw.githubusercontent.com/tilt-dev/tilt/master/scripts/install.sh | sudo bash; fi".to_string());
    }
    lines.join("\n")
}

fn prepare_codex_config(
    state: &State,
    policy: &Policy,
    auth_plan: &CodexAuthPlan,
) -> CliResult<PathBuf> {
    ensure_dir(&state.paths.codex_dir)?;
    let config = format!(
        "# Generated by SafeDev.\n# SafeDev protects your Mac. Codex works inside SafeDev.\nsandbox_mode = \"workspace-write\"\napproval_policy = \"on-request\"\nwritable_roots = [{}]\n\n[safe_dev]\nmode = {}\nhost_home_mounted = false\ndocker_socket_mounted = false\nproject = {}\n",
        json_str(&state.project_vm_path),
        json_str(&state.mode),
        json_str(&state.project_vm_path)
    );
    write_text(&state.paths.codex_dir.join("config.toml"), &config)?;
    let source = match auth_plan {
        CodexAuthPlan::Import { source, .. } => *source,
        CodexAuthPlan::ReuseStaged => "existing safedev auth.json",
        CodexAuthPlan::ReuseVm => "existing vm auth.json",
        CodexAuthPlan::None => "none",
    };
    let broker = format!(
        "{{\n  \"broker\": \"safedev\",\n  \"issuedAt\": {},\n  \"duration\": {},\n  \"ambientSecrets\": false,\n  \"source\": {},\n  \"note\": \"Host ~/.codex is not mounted. Run codex login inside SafeDev or provide an explicit brokered session.\"\n}}\n",
        json_str(&now_stamp()),
        json_str(&policy.default_duration),
        json_str(source)
    );
    write_text(&state.paths.codex_dir.join("broker.json"), &broker)?;
    if let CodexAuthPlan::Import { path, .. } = auth_plan {
        let destination = state.paths.codex_dir.join("auth.json");
        fs::copy(path, &destination)
            .map_err(|error| format!("failed to copy {}: {error}", path.display()))?;
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o600)).map_err(|error| {
            format!(
                "failed to set permissions on {}: {error}",
                destination.display()
            )
        })?;
    }
    Ok(state.paths.codex_dir.clone())
}

fn install_codex_config(state: &State, codex_dir: &Path) -> CliResult {
    let remote_tmp = format!("/tmp/safedev-codex-{}", state.id);
    let prepare = run_shell_in_instance(
        state,
        &format!(
            "rm -rf {} && mkdir -p {}",
            shell_quote(&remote_tmp),
            shell_quote(&remote_tmp)
        ),
    )?;
    if !prepare.status.success() {
        print_output(&prepare);
        process::exit(prepare.status.code().unwrap_or(1));
    }

    let source = format!("{}/.", codex_dir.to_string_lossy());
    let copy_output = copy_into_instance(state, &source, &remote_tmp)?;
    if !copy_output.status.success() {
        print_output(&copy_output);
        process::exit(copy_output.status.code().unwrap_or(1));
    }

    let install = run_shell_in_instance(
        state,
        &format!(
            "sudo mkdir -p /home/dev/.codex && sudo cp -a {tmp}/. /home/dev/.codex/ && sudo chown -R dev:dev /home/dev/.codex && sudo chmod 700 /home/dev /home/dev/.codex && if [ -f /home/dev/.codex/auth.json ]; then sudo chmod 600 /home/dev/.codex/auth.json; fi && rm -rf {tmp}",
            tmp = shell_quote(&remote_tmp)
        ),
    )?;
    if !install.status.success() {
        print_output(&install);
        process::exit(install.status.code().unwrap_or(1));
    }
    Ok(())
}

fn codex_launch_argv(codex_args: &[String], extra_env: &[(String, String)]) -> Vec<String> {
    let mut command = vec!["codex".to_string()];
    command.extend(codex_args.iter().cloned());
    let mut sudo_command = vec![
        "sudo".to_string(),
        "-H".to_string(),
        "-u".to_string(),
        "dev".to_string(),
        "env".to_string(),
    ];
    sudo_command.extend(
        extra_env
            .iter()
            .map(|(key, value)| format!("{key}={value}")),
    );
    sudo_command.extend([
        "bash".to_string(),
        "-lc".to_string(),
        format!(
            "export NPM_CONFIG_PREFIX=/home/dev/.npm-global; export PATH=/home/dev/.npm-global/bin:$PATH; mkdir -p /home/dev/.npm-global; if ! command -v codex >/dev/null 2>&1; then npm install -g @openai/codex; fi; exec {}",
            command
                .iter()
                .map(|arg| shell_quote(arg))
                .collect::<Vec<String>>()
                .join(" ")
        ),
    ]);
    sudo_command
}

fn network_env(policy: &Policy) -> Vec<(String, String)> {
    vec![
        (
            "SAFEDEV_NETWORK_MODE".to_string(),
            policy.network_mode.clone(),
        ),
        (
            "SAFEDEV_EGRESS_LOG".to_string(),
            "/var/log/safedev/egress.log".to_string(),
        ),
        (
            "SAFEDEV_BLOCK_METADATA_IPS".to_string(),
            policy.blocked_metadata_ips.join(","),
        ),
        (
            "SAFEDEV_NETWORK_ALLOWLIST".to_string(),
            policy.network_allowlist.join(","),
        ),
        (
            "HTTP_PROXY".to_string(),
            "http://127.0.0.1:18080".to_string(),
        ),
        (
            "HTTPS_PROXY".to_string(),
            "http://127.0.0.1:18080".to_string(),
        ),
        (
            "http_proxy".to_string(),
            "http://127.0.0.1:18080".to_string(),
        ),
        (
            "https_proxy".to_string(),
            "http://127.0.0.1:18080".to_string(),
        ),
        (
            "NO_PROXY".to_string(),
            "localhost,127.0.0.1,::1".to_string(),
        ),
        (
            "no_proxy".to_string(),
            "localhost,127.0.0.1,::1".to_string(),
        ),
    ]
}

fn limactl_path() -> String {
    env::var("SAFEDEV_LIMACTL").unwrap_or_else(|_| "limactl".to_string())
}

fn run_limactl(args: &[String]) -> CliResult<Output> {
    Command::new(limactl_path())
        .args(args)
        .output()
        .map_err(limactl_spawn_error)
}

fn run_limactl_with_progress(args: &[String], progress: &mut UpProgress) -> CliResult<Output> {
    if !progress.enabled() {
        return run_limactl(args);
    }

    let mut child = Command::new(limactl_path())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(limactl_spawn_error)?;

    let (tx, rx) = mpsc::channel();
    let mut readers = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        readers.push(read_command_stream(
            stdout,
            CommandStream::Stdout,
            tx.clone(),
        ));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(read_command_stream(
            stderr,
            CommandStream::Stderr,
            tx.clone(),
        ));
    }
    drop(tx);

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    loop {
        while let Ok(chunk) = rx.try_recv() {
            collect_command_chunk(chunk, &mut stdout, &mut stderr, progress);
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("failed to wait for {}: {error}", limactl_path()))?
        {
            for reader in readers {
                let _ = reader.join();
            }
            while let Ok(chunk) = rx.try_recv() {
                collect_command_chunk(chunk, &mut stdout, &mut stderr, progress);
            }
            return Ok(Output {
                status,
                stdout,
                stderr,
            });
        }
        progress.pulse();
        thread::sleep(Duration::from_millis(120));
    }
}

fn read_command_stream<R: Read + Send + 'static>(
    reader: R,
    stream: CommandStream,
    tx: mpsc::Sender<CommandChunk>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        loop {
            let mut bytes = Vec::new();
            match reader.read_until(b'\n', &mut bytes) {
                Ok(0) => break,
                Ok(_) => {
                    if tx.send(CommandChunk { stream, bytes }).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
}

fn collect_command_chunk(
    chunk: CommandChunk,
    stdout: &mut Vec<u8>,
    stderr: &mut Vec<u8>,
    progress: &mut UpProgress,
) {
    match chunk.stream {
        CommandStream::Stdout => stdout.extend_from_slice(&chunk.bytes),
        CommandStream::Stderr => stderr.extend_from_slice(&chunk.bytes),
    }
    let text = String::from_utf8_lossy(&chunk.bytes).replace('\r', "\n");
    for line in text.lines() {
        let line = line.trim();
        if !line.is_empty() {
            progress.log(line.to_string());
        }
    }
}

fn run_limactl_inherit(args: &[String]) -> CliResult<std::process::ExitStatus> {
    Command::new(limactl_path())
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(limactl_spawn_error)
}

fn limactl_spawn_error(error: io::Error) -> String {
    if error.kind() == io::ErrorKind::NotFound {
        format!(
            "Failed to run {}: {}. Install Lima first with \"brew install lima\", or set SAFEDEV_LIMACTL for tests.",
            limactl_path(),
            error
        )
    } else {
        format!("Failed to run {}: {}", limactl_path(), error)
    }
}

fn start_instance_with_progress(state: &State, progress: &mut UpProgress) -> CliResult<Output> {
    run_limactl_with_progress(&start_instance_args(state), progress)
}

fn start_instance_args(state: &State) -> Vec<String> {
    vec![
        "start".to_string(),
        "--tty=false".to_string(),
        "--name".to_string(),
        state.instance_name.clone(),
        state.paths.lima_file.to_string_lossy().to_string(),
    ]
}

fn start_existing_instance_with_progress(
    state: &State,
    progress: &mut UpProgress,
) -> CliResult<Output> {
    run_limactl_with_progress(&start_existing_instance_args(state), progress)
}

fn start_existing_instance_args(state: &State) -> Vec<String> {
    vec![
        "start".to_string(),
        "--tty=false".to_string(),
        state.instance_name.clone(),
    ]
}

fn lima_instance_status(instance_name: &str) -> CliResult<Option<String>> {
    let output = run_limactl(&[
        "list".to_string(),
        "--format".to_string(),
        "{{.Name}} {{.Status}}".to_string(),
    ])?;
    if !output.status.success() {
        print_output(&output);
        process::exit(output.status.code().unwrap_or(1));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(instance_name) {
            return Ok(parts.next().map(str::to_string));
        }
    }
    Ok(None)
}

fn delete_instance(state: &State) -> CliResult<Output> {
    run_limactl(&[
        "delete".to_string(),
        "-f".to_string(),
        state.instance_name.clone(),
    ])
}

fn run_shell_in_instance(state: &State, command: &str) -> CliResult<Output> {
    run_limactl(&shell_in_instance_args(state, command))
}

fn run_shell_in_instance_with_progress(
    state: &State,
    command: &str,
    progress: &mut UpProgress,
) -> CliResult<Output> {
    run_limactl_with_progress(&shell_in_instance_args(state, command), progress)
}

fn shell_in_instance_args(state: &State, command: &str) -> Vec<String> {
    vec![
        "shell".to_string(),
        "--workdir".to_string(),
        "/".to_string(),
        state.instance_name.clone(),
        "--".to_string(),
        "bash".to_string(),
        "-lc".to_string(),
        command.to_string(),
    ]
}

fn copy_into_instance(state: &State, source: &str, destination: &str) -> CliResult<Output> {
    run_limactl(&[
        "copy".to_string(),
        source.to_string(),
        format!("{}:{destination}", state.instance_name),
    ])
}

fn shell_interactive_args(state: &State) -> Vec<String> {
    vec![
        "shell".to_string(),
        "--tty=true".to_string(),
        "--workdir".to_string(),
        state.project_vm_path.clone(),
        state.instance_name.clone(),
        "--".to_string(),
        "sudo".to_string(),
        "-H".to_string(),
        "-u".to_string(),
        "dev".to_string(),
        "env".to_string(),
        "HOME=/home/dev".to_string(),
        "NPM_CONFIG_PREFIX=/home/dev/.npm-global".to_string(),
        "PATH=/home/dev/.npm-global/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
        "bash".to_string(),
        "-l".to_string(),
    ]
}

fn shell_args(
    state: &State,
    command_argv: &[String],
    extra_env: &[(String, String)],
) -> Vec<String> {
    let env_prefix = extra_env
        .iter()
        .map(|(key, value)| format!("{key}={}", shell_quote(value)))
        .collect::<Vec<String>>()
        .join(" ");
    let command = command_argv
        .iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<String>>()
        .join(" ");
    let wrapped = if env_prefix.is_empty() {
        format!("cd {} && {}", shell_quote(&state.project_vm_path), command)
    } else {
        format!(
            "cd {} && {} {}",
            shell_quote(&state.project_vm_path),
            env_prefix,
            command
        )
    };
    vec![
        "shell".to_string(),
        "--workdir".to_string(),
        state.project_vm_path.clone(),
        state.instance_name.clone(),
        "--".to_string(),
        "bash".to_string(),
        "-lc".to_string(),
        wrapped,
    ]
}

fn run_in_instance(
    state: &State,
    command_argv: &[String],
    extra_env: &[(String, String)],
) -> CliResult<Output> {
    run_limactl(&shell_args(state, command_argv, extra_env))
}

fn create_snapshot(state: &mut State, reason: &str) -> CliResult<Snapshot> {
    let label = format!("{}-{}", timestamp_label(), safe_name(reason));
    let root = state.paths.snapshots_dir.join(&label);
    let project_destination = root.join("project");
    copy_project(&state.project_host_path, &project_destination)?;
    let snapshot = Snapshot {
        label,
        reason: reason.to_string(),
        created_at: now_stamp(),
        project_path: project_destination.to_string_lossy().to_string(),
    };
    state.snapshots.push(snapshot.clone());
    Ok(snapshot)
}

fn restore_snapshot(state: &State, label: Option<&str>) -> CliResult<(Snapshot, PathBuf)> {
    let snapshot = if let Some(label) = label {
        state
            .snapshots
            .iter()
            .find(|snapshot| snapshot.label == label)
    } else {
        state.snapshots.last()
    }
    .ok_or_else(|| "No SafeDev snapshot is available to roll back.".to_string())?
    .clone();

    let backup_root = state
        .paths
        .rollback_backups_dir
        .join(format!("{}-before-rollback", timestamp_label()));
    copy_project(&state.project_host_path, &backup_root.join("project"))?;

    for entry in fs::read_dir(&state.project_host_path).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name().to_string_lossy().to_string();
        if excluded_top_level(&name) {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(&path)
                .map_err(|error| format!("failed to remove {}: {error}", path.display()))?;
        } else {
            fs::remove_file(&path)
                .map_err(|error| format!("failed to remove {}: {error}", path.display()))?;
        }
    }
    copy_project(Path::new(&snapshot.project_path), &state.project_host_path)?;
    Ok((snapshot, backup_root))
}

fn copy_project(source: &Path, destination: &Path) -> CliResult {
    copy_filtered(source, destination, source)
}

fn copy_filtered(source: &Path, destination: &Path, root: &Path) -> CliResult {
    let rel = source.strip_prefix(root).unwrap_or(source);
    if let Some(top) = rel.components().next() {
        let name = top.as_os_str().to_string_lossy();
        if excluded_top_level(&name) {
            return Ok(());
        }
    }

    let metadata = fs::symlink_metadata(source)
        .map_err(|error| format!("failed to stat {}: {error}", source.display()))?;
    if metadata.file_type().is_symlink() {
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        let target = fs::read_link(source)
            .map_err(|error| format!("failed to read symlink {}: {error}", source.display()))?;
        let _ = fs::remove_file(destination);
        unix_fs::symlink(target, destination).map_err(|error| {
            format!("failed to copy symlink {}: {error}", destination.display())
        })?;
    } else if metadata.is_dir() {
        ensure_dir(destination)?;
        for entry in fs::read_dir(source)
            .map_err(|error| format!("failed to read {}: {error}", source.display()))?
        {
            let entry = entry.map_err(|error| error.to_string())?;
            copy_filtered(&entry.path(), &destination.join(entry.file_name()), root)?;
        }
    } else {
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        fs::copy(source, destination).map_err(|error| {
            format!(
                "failed to copy {} to {}: {error}",
                source.display(),
                destination.display()
            )
        })?;
    }
    Ok(())
}

fn excluded_top_level(name: &str) -> bool {
    name == ".git" || name == "node_modules"
}

fn record_event(state: &State, event: InspectEvent) -> CliResult {
    let command_text = event.command.join(" ");
    let backend_text = event.backend_args.join(" ");
    let snapshot_label = event
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.label.as_str())
        .unwrap_or("none");
    let mut env_text = String::new();
    for (key, value) in [
        ("action", event.action.as_str()),
        ("command", command_text.as_str()),
        ("backend_args", backend_text.as_str()),
        ("snapshot_label", snapshot_label),
    ] {
        env_text.push_str(key);
        env_text.push('=');
        env_text.push_str(value);
        env_text.push('\n');
    }
    write_text(&state.paths.last_inspect_env, &env_text)?;

    let event_json = inspect_event_json(&event);
    write_text(&state.paths.last_inspect_file, &event_json)?;
    append_text(&state.paths.history_file, &event_json.replace('\n', ""))?;
    append_text(&state.paths.history_file, "\n")?;
    Ok(())
}

fn inspect_event_json(event: &InspectEvent) -> String {
    let snapshot = event
        .snapshot
        .as_ref()
        .map(|snapshot| {
            format!(
                "{{\"label\":{},\"reason\":{},\"createdAt\":{},\"projectPath\":{}}}",
                json_str(&snapshot.label),
                json_str(&snapshot.reason),
                json_str(&snapshot.created_at),
                json_str(&snapshot.project_path)
            )
        })
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\n  \"at\": {},\n  \"action\": {},\n  \"command\": {},\n  \"backendArgs\": {},\n  \"snapshot\": {},\n  \"codexConfig\": {},\n  \"backupRoot\": {}\n}}\n",
        json_str(&now_stamp()),
        json_str(&event.action),
        json_array(&event.command),
        json_array(&event.backend_args),
        snapshot,
        json_option(event.codex_config.as_deref()),
        json_option(event.backup_root.as_deref())
    )
}

fn format_last_inspect(state: &State) -> CliResult<String> {
    if !state.paths.last_inspect_env.exists() {
        return Err("No SafeDev inspect event found yet.".to_string());
    }
    let values = read_kv(&state.paths.last_inspect_env)?;
    let policy = build_policy(&state.mode);
    let action = required_kv(&values, "action")?;
    let command = values
        .get("command")
        .cloned()
        .unwrap_or_else(|| action.clone());
    let backend = values
        .get("backend_args")
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    let snapshot = values
        .get("snapshot_label")
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    Ok(format!(
        "SafeDev inspect: last\nProject: {}\nMode: {}\nAction: {}\nCommand: {}\nProcess tree: launched inside Lima instance {}\nFile writes: confined to {}; host home mounted={}; docker socket mounted={}\nSnapshot: {}\nNetwork: {}; metadata blocked={}; egress logging={}\nBackend call: {}",
        state.project_vm_path,
        state.mode,
        action,
        if command.is_empty() { action.clone() } else { command },
        state.instance_name,
        state.project_vm_path,
        policy.host_home,
        policy.docker_socket,
        snapshot,
        policy.network_mode,
        policy.block_metadata_ips,
        policy.log_egress,
        backend
    ))
}

fn read_kv(path: &Path) -> CliResult<HashMap<String, String>> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let mut values = HashMap::new();
    for line in text.lines() {
        if let Some((key, value)) = line.split_once('=') {
            values.insert(key.to_string(), value.to_string());
        }
    }
    Ok(values)
}

fn required_kv(values: &HashMap<String, String>, key: &str) -> CliResult<String> {
    values
        .get(key)
        .cloned()
        .ok_or_else(|| format!("state is missing {key}"))
}

fn print_output(output: &Output) {
    let _ = io::stdout().write_all(&output.stdout);
    let _ = io::stderr().write_all(&output.stderr);
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn ensure_dir(path: &Path) -> CliResult {
    fs::create_dir_all(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))
}

fn write_text(path: &Path, text: &str) -> CliResult {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    fs::write(path, text).map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn append_text(path: &Path, text: &str) -> CliResult {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    file.write_all(text.as_bytes())
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn json_escape(value: &str) -> String {
    let mut output = String::new();
    for current in value.chars() {
        match current {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            c if c.is_control() => output.push_str(&format!("\\u{:04x}", c as u32)),
            c => output.push(c),
        }
    }
    output
}

fn json_str(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

fn json_option(value: Option<&str>) -> String {
    value.map(json_str).unwrap_or_else(|| "null".to_string())
}

fn json_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| json_str(value))
            .collect::<Vec<String>>()
            .join(", ")
    )
}

fn compact_json(value: &str) -> String {
    value.split_whitespace().collect::<Vec<&str>>().join(" ")
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn safe_name(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for current in value.chars() {
        let next = current.to_ascii_lowercase();
        if next.is_ascii_alphanumeric() || next == '.' || next == '_' || next == '-' {
            output.push(next);
            last_dash = false;
        } else if !last_dash {
            output.push('-');
            last_dash = true;
        }
    }
    let trimmed = output.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "project".to_string()
    } else {
        trimmed
    }
}

fn stable_hash_hex(value: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn now_stamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn timestamp_label() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| format!("{}-{}", duration.as_secs(), duration.subsec_nanos()))
        .unwrap_or_else(|_| "0".to_string())
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "_/:=.,@%+-".contains(c))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn indent_block(text: &str, spaces: usize) -> String {
    let padding = " ".repeat(spaces);
    text.trim_end()
        .lines()
        .map(|line| format!("{padding}{line}"))
        .collect::<Vec<String>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_handles_spaces_and_quotes() {
        assert_eq!(shell_quote("pnpm"), "pnpm");
        assert_eq!(shell_quote("hello world"), "'hello world'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn policy_modes_match_product_shape() {
        let normal = build_policy("normal");
        assert_eq!(normal.network_mode, "monitored");
        assert_eq!(normal.install_scripts, "prompt");
        assert!(!normal.host_home);

        let locked = build_policy("locked");
        assert_eq!(locked.network_mode, "restricted");
        assert_eq!(locked.install_scripts, "block");

        let trusted = build_policy("trusted");
        assert_eq!(trusted.network_mode, "broad_monitored");
        assert!(trusted.sandbox_home_persistent);
        assert!(!trusted.host_home);
    }

    #[test]
    fn json_comment_stripper_keeps_urls() {
        let input = r#"{"name":"a//b", // comment
        "image":"repo/*x*/tag"}"#;
        let stripped = strip_json_comments(input);
        assert!(stripped.contains("\"a//b\""));
        assert!(stripped.contains("\"repo/*x*/tag\""));
        assert!(!stripped.contains("comment"));
    }
}
