#!/bin/sh
# larkline installer.
#
# Downloads one static binary from GitHub Releases, verifies its checksum and
# installs it without sudo. There is no npm, no Node and no postinstall step.
#
# Usage:
#   sh install.sh                     # latest release, ~/.local/bin
#   sh install.sh --version 0.2.0     # pin a version
#   sh install.sh --dir /usr/local/bin
#
# Environment:
#   LARKLINE_INSTALL_DIR   install directory (default ~/.local/bin)
#   LARKLINE_RELEASE_BASE  override the download base (mirror)
#   LARKLINE_RELEASE_REPO  override owner/repo

set -eu

REPO="${LARKLINE_RELEASE_REPO:-luozhouyang/larkline}"
VERSION="latest"
INSTALL_DIR="${LARKLINE_INSTALL_DIR:-$HOME/.local/bin}"

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="${2#v}"; shift 2 ;;
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$(uname -s)" in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  MINGW*|MSYS*|CYGWIN*) os=windows ;;
  *) echo "larkline: unsupported operating system: $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64) arch=x86_64 ;;
  arm64|aarch64) arch=aarch64 ;;
  *) echo "larkline: unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

case "$os" in
  darwin) arch=universal; ext=zip ;;
  linux) ext=tar.gz ;;
  windows) ext=zip ;;
esac

asset="larkline-${os}-${arch}.${ext}"
if [ "$VERSION" = "latest" ]; then
  base="https://github.com/${REPO}/releases/latest/download"
else
  base="https://github.com/${REPO}/releases/download/v${VERSION}"
fi
base="${LARKLINE_RELEASE_BASE:-$base}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

echo "larkline: downloading ${asset}"
curl -fsSL "$base/$asset" -o "$tmp/$asset"

if curl -fsSL "$base/SHA256SUMS" -o "$tmp/SHA256SUMS" 2>/dev/null; then
  if (cd "$tmp" && grep " ${asset}\$" SHA256SUMS > expected); then
    if command -v sha256sum >/dev/null 2>&1; then
      (cd "$tmp" && sha256sum -c expected)
    else
      (cd "$tmp" && shasum -a 256 -c expected)
    fi
  else
    echo "larkline: warning: ${asset} missing from SHA256SUMS" >&2
  fi
else
  echo "larkline: warning: checksums unavailable for this release" >&2
fi

echo "larkline: extracting"
case "$ext" in
  tar.gz) tar -xzf "$tmp/$asset" -C "$tmp" ;;
  zip) unzip -q "$tmp/$asset" -d "$tmp" ;;
esac

binary="$(find "$tmp" -maxdepth 3 -type f -name 'larkline*' ! -name '*.zip' ! -name '*.tar.gz' | head -n 1)"
if [ -z "$binary" ]; then
  echo "larkline: archive did not contain a larkline binary" >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
# Write beside the destination, then rename: a running daemon keeps the old
# inode and finishes its work, and the next start picks up the new binary.
cp "$binary" "$INSTALL_DIR/larkline.new"
chmod 755 "$INSTALL_DIR/larkline.new"
mv -f "$INSTALL_DIR/larkline.new" "$INSTALL_DIR/larkline"

echo "larkline: installed to $INSTALL_DIR/larkline"
case ":${PATH:-}:" in
  *":${INSTALL_DIR}:"*) ;;
  *) echo "larkline: add it to PATH:  export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac

"$INSTALL_DIR/larkline" --version
echo
echo "next: larkline doctor"
