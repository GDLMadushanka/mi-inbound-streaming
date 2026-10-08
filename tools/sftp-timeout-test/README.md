# Proving `transport.vfs.SFTPTimeout` bounds a stalled SFTP host

The reported hazard: an SFTP source that accepts the TCP connection and then stops responding
blocks the inbound's polling thread forever — including on the metadata calls made before a file
is ever opened, such as the `exists()` inside `Utils.isFailRecord`. jsch has no timeout by default.

This harness drives that exact VFS call (`resolveFile` + `exists()`) through a
[toxiproxy](https://github.com/Shopify/toxiproxy) proxy, with and without the timeout, and times
how long the thread is held.

## Requirements

`toxiproxy-server` and `toxiproxy-cli` on `PATH`, python3, a built `target/classes`, and an MI
install to borrow jars from. No MI server needs to be running.

## 1. Stalled connect — no external service needed

```
tools/sftp-timeout-test/run.sh <MI_HOME>
```

A local black hole (`blackhole.py`) accepts connections and sends nothing; toxiproxy fronts it.

Observed on MI 4.1.0 with commons-vfs `2.2.0-wso2v13.20`:

```
=== WITHOUT a timeout (watchdog 45s) ===
  SFTPTimeout = <unset>
  INFO: Connecting to 127.0.0.1 port 2222
  INFO: Connection established
  STILL BLOCKED after 45097ms  <-- the poller thread is stuck

=== WITH transport.vfs.SFTPTimeout=5000 (watchdog 45s) ===
  SFTPTimeout = 5000ms
  INFO: Connection established
  INFO: Disconnecting from 127.0.0.1 port 2222
  unblocked after 5239ms (threw SocketTimeoutException: Read timed out)

PASS - unset blocks past the watchdog; SFTPTimeout releases the thread
```

The watchdog is what ends the first run; left alone it does not return.

## 2. Real SFTP server, delay switched on and off

Covers the literal case in the report — an established session whose directory operation stalls —
and also shows that a configured timeout does **not** disturb healthy traffic. Any SFTP endpoint
works; a disposable one from <https://sftpcloud.io/tools/free-sftp-server> is enough.

```
tools/sftp-timeout-test/toxic-cycle.sh <MI_HOME> <host> <port> <user> <pass> <remoteDir> <remoteFile>
```

It puts toxiproxy in front of the real host and walks six phases, toggling toxics between them:

| # | toxiproxy | `SFTPTimeout` | expected |
|---|---|---|---|
| 1 | none | 10000 | `list` and `read` succeed |
| 2 | `latency=20000` | 5000 | fails in ~5s |
| 3 | latency removed | 5000 | succeeds again |
| 4 | `timeout=0` (hard stall) | *unset* | **blocked** past the watchdog |
| 5 | `timeout=0` | 5000 | fails in ~5s |
| 6 | stall removed | 5000 | succeeds again |

Measured against a free sftpcloud.io endpoint, `op=list` (which maps onto `ChannelSftp.ls`):

```
1. delay OFF   - OK in 4178ms - 1 entries: good-orders.csv
                 OK in 3315ms - 84 bytes read
2. delay ON    - failed after 5252ms: SocketTimeoutException: Read timed out
3. delay OFF   - OK in 3739ms  /  OK in 3198ms - 84 bytes read
4. stall ON, timeout unset - STILL BLOCKED after 30137ms  <-- the poller thread is stuck
5. stall ON, timeout 5000  - failed after 5225ms: SocketTimeoutException: Read timed out
6. stall OFF   - OK in 4024ms

PASS - the timeout bounds a stalled server and healthy traffic is unaffected
```

Phase 4 is the defect: the session is up, the server stops answering, and the thread never
returns — the 30s figure is only where the watchdog gives up. Phases 3 and 6 matter just as much:
they show the timeout costs nothing when the server is behaving.

Credentials are command-line arguments. Nothing in this directory writes them to disk — do not
commit them, and treat a server whose details have been shared as disposable.

## What the result means

`SFTPTimeout` maps to `SftpFileSystemConfigBuilder.setTimeout`, which jsch applies as the session's
socket `SO_TIMEOUT`. Any read that stalls longer than it fails instead of hanging.

Pick the value with care: it also bounds reads taken **while a file is streaming**, and those are
paced by how fast the sequence mediates each record rather than by the network. Set it above the
slowest gap between two reads, not just above the round-trip time. That interaction is why the
parameter has no default.
