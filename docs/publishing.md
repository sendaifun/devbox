# Publishing SafeDev

This project publishes from the source repo:

```text
git@github.com:sendaifun/devbox.git
```

The crate and CLI are currently named `safedev`.

## Rust / Cargo

`crates.io` publishes source packages. Users install by compiling locally:

```bash
cargo install safedev --locked
```

Release checklist:

```bash
cargo check
cargo test
cargo publish --dry-run
cargo login
cargo publish
```

## Homebrew

Homebrew publishes through the SendAI tap:

```text
git@github.com:sendaifun/homebrew-tap.git
```

The user-facing install command is:

```bash
brew install sendaifun/tap/safedev
```

## First Homebrew Release

1. Tag the source repo:

```bash
git tag v0.1.0
git push origin v0.1.0
```

2. Download the source archive and compute its checksum:

```bash
curl -L -o safedev-0.1.0.tar.gz \
  https://github.com/sendaifun/devbox/archive/refs/tags/v0.1.0.tar.gz

shasum -a 256 safedev-0.1.0.tar.gz
```

3. Copy `packaging/homebrew/safedev.rb.template` into the tap repo as:

```text
Formula/safedev.rb
```

4. Replace:

```text
__VERSION__ -> 0.1.0
__SHA256__ -> the tarball sha256
```

5. Test locally from the tap repo:

```bash
brew install --build-from-source ./Formula/safedev.rb
brew test safedev
brew audit --strict --online safedev
```

6. Push the tap repo:

```bash
git push origin main
```

The v0.1.0 formula has already been published with:

```text
sha256 d0b940744c8413116fb9db539e7139663e371f26030dcb072c05049fe6f6c35b
```

## Bottles

The initial formula builds from source and depends on Rust at build time. Later, enable bottles in the tap repo so Homebrew can install prebuilt binaries for common macOS targets.
