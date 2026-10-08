#!/usr/bin/env bash
# Prove transport.vfs.SFTPTimeout bounds a stalled SFTP host.
#
#   ./run.sh <MI_HOME>                       - stalled CONNECT, against a local black hole
#   ./run.sh <MI_HOME> <host:port> <user> <pass> [remotePath]
#                                            - stalled MID-SESSION, against a real SFTP server
#                                              reached through toxiproxy
#
# Both routes put toxiproxy in front, so the stall is injected rather than simulated by the server.
set -u
MI_HOME="${1:?usage: $0 <MI_HOME> [host:port user pass [remotePath]]}"
UPSTREAM="${2:-}"; SUSER="${3:-demo}"; SPASS="${4:-demo}"; RPATH="${5:-/}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PROXY_PORT=2222
BH_PORT=2022
API=http://127.0.0.1:8474

P="$MI_HOME/wso2/components/plugins"
pick() { ls "$P"/$1 2>/dev/null | head -1; }
CP="$ROOT/target/classes"
for j in "commons-vfs2_*.jar" "jsch_*.jar" "com.google.gson_*.jar" \
         "org.apache.commons.commons-io_*.jar" "commons-lang3_*.jar" "commons-lang_*.jar" \
         "commons-net_*.jar" "smbj_*.jar" "asn-one_*.jar"; do
  f=$(pick "$j"); [ -n "$f" ] && CP="$CP:$f"
done
# The server's commons-logging is pax-logging-api, which needs an OSGi framework to initialise.
# Outside the container use a plain one.
JCL=$(find "${M2:-$HOME/m2_temp3}" -path "*/commons-logging/commons-logging/*/commons-logging-*.jar" \
        2>/dev/null | grep -v sources | head -1)
[ -n "$JCL" ] || { echo "no plain commons-logging jar found; set M2 to a repo that has one"; exit 2; }
CP="$CP:$JCL"
SLF=$(find "${M2:-$HOME/m2_temp3}" -path "*/org/slf4j/slf4j-api/*/slf4j-api-*.jar" 2>/dev/null \
        | grep -v sources | head -1)
[ -n "$SLF" ] && CP="$CP:$SLF"
[ -d "$ROOT/target/classes" ] || { echo "build first"; exit 2; }

cleanup() {
  toxiproxy-cli delete sftp_stall >/dev/null 2>&1
  [ -n "${BH_PID:-}" ] && kill "$BH_PID" 2>/dev/null
  [ -n "${TOXI_PID:-}" ] && kill "$TOXI_PID" 2>/dev/null
}
trap cleanup EXIT

# toxiproxy
if ! curl -s -o /dev/null "$API/version"; then
  nohup toxiproxy-server >/tmp/toxiproxy.log 2>&1 & TOXI_PID=$!
  for _ in $(seq 1 20); do curl -s -o /dev/null "$API/version" && break; sleep .25; done
fi

if [ -z "$UPSTREAM" ]; then
  MODE="stalled connect (local black hole)"
  python3 "$HERE/blackhole.py" "$BH_PORT" >/tmp/blackhole.log 2>&1 & BH_PID=$!
  sleep 1
  UPSTREAM="127.0.0.1:$BH_PORT"
  TOXIC=""
else
  MODE="stalled mid-session (real SFTP via toxiproxy timeout toxic)"
  TOXIC="yes"
fi

toxiproxy-cli delete sftp_stall >/dev/null 2>&1
toxiproxy-cli create -l "127.0.0.1:$PROXY_PORT" -u "$UPSTREAM" sftp_stall >/dev/null
echo "mode: $MODE"
echo "proxy: 127.0.0.1:$PROXY_PORT -> $UPSTREAM"

javac -cp "$CP" -d "$HERE" "$HERE/SftpTimeoutProbe.java" 2>&1 | head -5
URI="sftp://$SUSER:$SPASS@127.0.0.1:$PROXY_PORT$RPATH"

run() { java -cp "$HERE:$CP" SftpTimeoutProbe "$URI" "$1" "$2"; }

if [ -n "$TOXIC" ]; then
  echo
  echo "--- sanity: proxy healthy, the endpoint should answer ---"
  run 10000 30 || true
  echo
  echo "--- injecting the stall (timeout toxic: hold the connection, send nothing) ---"
  toxiproxy-cli toxic add -t timeout -a timeout=0 -n stall sftp_stall >/dev/null
fi

echo
echo "=== WITHOUT a timeout (watchdog 45s) ==="
run none 45; without=$?
echo
echo "=== WITH transport.vfs.SFTPTimeout=5000 (watchdog 45s) ==="
run 5000 45; with=$?

echo
if [ "$without" -ne 0 ] && [ "$with" -eq 0 ]; then
  echo "PASS - unset blocks past the watchdog; SFTPTimeout releases the thread"
  exit 0
fi
echo "INCONCLUSIVE - without=$without with=$with (expected without=1 with=0)"
exit 1
