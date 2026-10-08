# Testing SFTP timeouts by hand

toxiproxy sits between MI and the SFTP server, so the stall is switched on and off without
touching the server.

```
MI inbound ──► toxiproxy 127.0.0.1:2222 ──► <sftp host>:22
                    ▲
             toxics added/removed here
```

## 0. Prerequisites

A reachable SFTP server with a file to read, and toxiproxy running:

```bash
toxiproxy-server &                 # skip if `brew services list` shows it started
curl -s http://127.0.0.1:8474/version && echo   # should print a version
```

## 1. Point a proxy at the SFTP server

```bash
toxiproxy-cli create -l 127.0.0.1:2222 -u <sftp-host>:22 sftp_mi
toxiproxy-cli list
```

## 2. Deploy the inbound

Copy `sftpTimeoutInbound.xml`, `sftpTimeoutSeq.xml` and `sftpTimeoutFaultSeq.xml` into
`<MI_HOME>/repository/deployment/server/synapse-configs/default/{inbound-endpoints,sequences}/`,
replacing `SFTP_USER:SFTP_PASS` with real credentials. It polls `127.0.0.1:2222` — the proxy —
and sets:

```xml
<parameter name="transport.vfs.SFTPTimeout">5000</parameter>
<parameter name="transport.vfs.SFTPConnectTimeout">5000</parameter>
```

Start MI and tail the log:

```bash
<MI_HOME>/bin/micro-integrator.sh &
tail -f <MI_HOME>/repository/logs/wso2carbon.log | grep -viE "heartbeat"
```

## 3. Baseline — no toxic

The file is read:

```
INFO {inboundendpoint:sftpTimeoutInbound} SFTP_ROW = 1, data = {"order_id":"1",...}
INFO Streaming completed for good-orders.csv: processed=3, parseFailed=0, mediationFailed=0
```

## 4. Enable a delay

```bash
toxiproxy-cli toxic add -t latency -a latency=20000 -n delay sftp_mi
```

20s of added latency against a 5s timeout. Within a poll or two:

```
INFO  Connecting to 127.0.0.1 port 2222
INFO  Connection established
INFO  Disconnecting from 127.0.0.1 port 2222          <-- 5.0s after connecting
ERROR Cannot get the lock for the file : sftp://user:***@127.0.0.1:2222/test/good-orders.csv
WARN  Failed to resolve the file URI: sftp://user:***@127.0.0.1:2222/test, in attempt 1
```

The point is the **5.0s gap** between "Connection established" and "Disconnecting", and that
polling carries on afterwards. Comment the two timeout parameters out and repeat: the connection
is established and never torn down, and the poll never comes back.

## 5. Disable the delay

```bash
toxiproxy-cli toxic delete -n delay sftp_mi
```

The next poll succeeds again — proof the timeout costs nothing when the server behaves:

```
INFO {inboundendpoint:sftpTimeoutInbound} SFTP_ROW = 1, data = {"order_id":"1",...}
INFO Streaming completed for good-orders.csv: processed=3, parseFailed=0, mediationFailed=0
```

## 6. Other toxics worth trying

```bash
# hard stall: hold the connection open, pass nothing (closest to the reported fault)
toxiproxy-cli toxic add -t timeout -a timeout=0 -n stall sftp_mi
toxiproxy-cli toxic delete -n stall sftp_mi

# slow trickle rather than a freeze
toxiproxy-cli toxic add -t bandwidth -a rate=1 -n slow sftp_mi

# take the whole proxy down - connection refused, not a stall (a different failure)
toxiproxy-cli toggle sftp_mi

toxiproxy-cli inspect sftp_mi     # what is currently attached
```

## 7. Clean up

```bash
toxiproxy-cli delete sftp_mi
```

## Measured results

| phase | toxic | `SFTPTimeout` | outcome |
|---|---|---|---|
| 1 | none | 5000 | 3 rows read |
| 2 | `latency=20000` | 5000 | disconnect 5.009s after connect, poll retries |
| 3 | removed | 5000 | 3 rows read again |

And from `toxic-cycle.sh`, which drives the same VFS calls without MI: with the timeout unset
and a hard stall, the thread was **still blocked after 30s**; with it set, it failed in 5.2s.
