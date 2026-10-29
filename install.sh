#!/bin/sh
# Installs the sessionbus binary from the latest GitHub release.
#   curl -fsSL https://raw.githubusercontent.com/ali-mosavian/sessionbus/main/install.sh | sh
# Env: BASE_URL (override the download location), PREFIX (default ~/.local/bin), VERSION (default latest), LIBC=gnu to prefer glibc on Linux.
set -eu

repo=ali-mosavian/sessionbus
prefix=${PREFIX:-$HOME/.local/bin}

case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  arm64 | aarch64) arch=aarch64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
case "$(uname -s)" in
  Darwin)
    [ "$arch" = aarch64 ] || { echo "only Apple silicon is built for macOS" >&2; exit 1; }
    target=aarch64-apple-darwin ;;
  Linux)
    # the musl build is static and runs on every distro
    target="$arch-unknown-linux-${LIBC:-musl}" ;;
  *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

if [ -n "${BASE_URL:-}" ]; then
  base=$BASE_URL
elif [ -n "${VERSION:-}" ]; then
  base=https://github.com/$repo/releases/download/$VERSION
else
  base=https://github.com/$repo/releases/latest/download
fi
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
file=sessionbus-$target.tar.gz

curl -fsSL "$base/$file" -o "$tmp/$file"
curl -fsSL "$base/SHA256SUMS" -o "$tmp/SHA256SUMS"
want=$(awk -v f="$file" '$2 == f { print $1 }' "$tmp/SHA256SUMS")
got=$( (sha256sum "$tmp/$file" 2>/dev/null || shasum -a 256 "$tmp/$file") | awk '{ print $1 }')
if [ -z "$want" ] || [ "$want" != "$got" ]; then
  echo "checksum mismatch for $file" >&2
  exit 1
fi

tar -xzf "$tmp/$file" -C "$tmp"
mkdir -p "$prefix"
install -m 755 "$tmp/sessionbus-$target/sessionbus" "$prefix/sessionbus"
echo "installed $prefix/sessionbus"
case ":$PATH:" in *":$prefix:"*) ;; *) echo "add $prefix to your PATH" ;; esac
