#!/usr/bin/env bash
# Drive a real SFTP server through toxiproxy, switching a delay on and off, and show what
# transport.vfs.SFTPTimeout does at each step.
#
#   ./toxic-cycle.sh <MI_HOME> <host> <port> <user> <pass> <remoteDir> <remoteFile>
#
# Credentials are arguments only - nothing here writes them to disk.
set -u
MI_HOME="${1:?usage: $0 <MI_HOME> <host> <port> <user> <pass> <remoteDir> <remoteFile>}"
SHOST="$2"; SPORT="$3"; SUSER="$4"; SPASS="$5"; RDIR="$6"; RFILE="$7"

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PROXY=127.0.0.1:2222
NAME=sftp_cycle
API=http://127.0.0.1:8474

P="$MI_HOME/wso2/components/plugins"
CP="$ROOT/target/classes"
for j in commons-vfs2_ jsch_ com.google.gson_ org.apache.commons.commons-io_ \
         commons-lang3_ commons-lang_ commons-net_ smbj_ asn-one_; do
  f=$(ls "$P"/${j}*.jar 2>/dev/null | head -1); [ -n "$f" ] && CP="$CP:$f"
done
CP="$CP:$(find "${M2:-$HOME/m2_temp3}" -path '*/commons-logging/commons-logging/*/commons-logging-*.jar' 2>/dev/null | grep -v sources | head -1)"
CP="$CP:$(find "${M2:-$HOME/m2_temp3}" -path '*/org/slf4j/slf4j-api/*/slf4j-api-*.jar' 2>/dev/null | grep -v sources | head -1)"

cleanup() {
  toxiproxy-cli delete "$NAME" >/dev/null 2>&1
  [ -n "${TOXI_PID:-}" ] && kill "$TOXI_PID" 2>/dev/null
}
trap cleanup EXIT

curl -s -o /dev/null "$API/version" || {
  nohup toxiproxy-server >/tmp/toxiproxy.log 2>&1 & TOXI_PID=$!
  for _ in $(seq 1 20); do curl -s -o /dev/null "$API/version" && break; sleep .25; done
}
toxiproxy-cli delete "$NAME" >/dev/null 2>&1
toxiproxy-cli create -l "$PROXY" -u "$SHOST:$SPORT" "$NAME" >/dev/null
javac -cp "$CP" -d "$HERE" "$HERE/SftpOpProbe.java" 2>&1 | head -3

BASE="sftp://$SUSER:$SPASS@$PROXY"
quiet() { grep -vE "^[A-Z][a-z]{2} [0-9]{2}, [0-9]{4}|^INFO:"; }
# Capture the exit status of java itself: piping straight into the filter would report the
# filter's status instead, which silently turns a blocked run into a pass.
probe() {
  local out rc
  out=$(java -cp "$HERE:$CP" SftpOpProbe "$1" "$2" "$3" "$4" 2>&1); rc=$?
  printf '%s\n' "$out" | quiet
  return $rc
}

delay_on()  { toxiproxy-cli toxic add    -t latency -a latency="$1" -n delay "$NAME" >/dev/null && \
              echo "  [toxiproxy] delay ENABLED  (latency=${1}ms)"; }
delay_off() { toxiproxy-cli toxic delete -n delay "$NAME" >/dev/null 2>&1 && \
              echo "  [toxiproxy] delay DISABLED"; }
stall_on()  { toxiproxy-cli toxic add    -t timeout -a timeout=0 -n stall "$NAME" >/dev/null && \
              echo "  [toxiproxy] hard stall ENABLED (connection held, no data)"; }
stall_off() { toxiproxy-cli toxic delete -n stall "$NAME" >/dev/null 2>&1 && \
              echo "  [toxiproxy] hard stall DISABLED"; }

echo "proxy $PROXY -> $SHOST:$SPORT"
echo
echo "========== 1. delay OFF - healthy baseline =========="
probe "$BASE/$RDIR" 10000 60 list
probe "$BASE/$RDIR/$RFILE" 10000 60 read
echo
echo "========== 2. delay ON (20s) vs SFTPTimeout=5000 =========="
delay_on 20000
probe "$BASE/$RDIR" 5000 60 list; a=$?
echo
echo "========== 3. delay OFF again - does it recover? =========="
delay_off
probe "$BASE/$RDIR" 5000 60 list; b=$?
probe "$BASE/$RDIR/$RFILE" 5000 60 read
echo
echo "========== 4. hard stall, SFTPTimeout UNSET - the bug =========="
stall_on
probe "$BASE/$RDIR" none 30 list; c=$?
echo
echo "========== 5. hard stall, SFTPTimeout=5000 - the fix =========="
probe "$BASE/$RDIR" 5000 30 list; d=$?
echo
echo "========== 6. stall OFF - recovered =========="
stall_off
probe "$BASE/$RDIR" 5000 60 list; e=$?

echo
echo "---------------- verdict ----------------"
ok=1
[ "$a" -eq 0 ] || { echo "FAIL: delayed list should have failed fast, not blocked"; ok=0; }
[ "$b" -eq 0 ] || { echo "FAIL: did not recover after the delay was removed"; ok=0; }
[ "$c" -eq 1 ] || { echo "FAIL: unset timeout should have stayed blocked (got $c)"; ok=0; }
[ "$d" -eq 0 ] || { echo "FAIL: SFTPTimeout did not release the thread"; ok=0; }
[ "$e" -eq 0 ] || { echo "FAIL: did not recover after the stall was removed"; ok=0; }
[ "$ok" -eq 1 ] && echo "PASS - the timeout bounds a stalled server and healthy traffic is unaffected"
exit $(( ok == 1 ? 0 : 1 ))
