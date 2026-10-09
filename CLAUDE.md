# mi-inbound-streaming

A VFS file inbound endpoint for **WSO2 MI 4.1.0** that streams large files (CSV, JSON, JSONL, text)
into a sequence one record or one chunk of records at a time, instead of loading the whole file into
one message. It is a port of `mi-inbound-file` (branch `streaming`). Read `PORTING-4.1.0.md` for
what differs from upstream and why: no message variables, unrelocated commons-vfs 2.2, a smaller
`GenericPollingConsumer`, and pinned Gson behaviour.

Inbound class: `org.wso2.carbon.inbound.streaming.VFSConsumer`.

## Build, test, deploy

```bash
mvn clean package                    # jar: target/mi-inbound-streaming-1.0.0.jar
mvn test                             # unit tests (JUnit 4)
tools/check-runtime-api.sh <MI_HOME> # verify every commons-vfs call exists in the server's jar
```

If your default `~/.m2` cannot resolve the WSO2 artifacts, point Maven at a repo that can, for
example `-Dmaven.repo.local=$HOME/m2_temp3`.

To deploy, copy the jar and `commons-csv-1.14.1.jar` into `<MI_HOME>/lib` and **restart** MI. Jars
in `lib` are not hot-deployed, and replacing one under a running server is unsafe. Inbound and
sequence XML files *are* hot-deployed. Note that `mvn test` does not rebuild the jar.

- **Java level:** the code targets Java 8.
- **MI runtime:** the server runs on whatever JDK it is started with. A JDK 8 server uses Nashorn
  for `<script language="js">`, and performance differs noticeably from newer JDKs.

## Where things are

| Path | Role |
|---|---|
| `VFSConsumer` | Polls the folder, locks, filters (`filter/`), hands each file to a handler, post-processes (move/delete) |
| `StreamInjectHandler` | The streaming loop: read, prepare, mediate, commit; error sidecars; reply file; checkpoints; completion callbacks |
| `OrderedWindow` | Bounded, in-order executor behind `StreamingParallelism` (see below) |
| `csv/`, `json/`, `jsonl/`, `text/` | Format processors: record and chunk iterators that build `StreamRecord` / `StreamChunk` |
| `csv/CsvChunkPayload` | Deferred JSON for a CSV chunk, built on the worker rather than the reader |
| `csv/CsvDataTypes` | Per-column JSON typing (`StreamingCsvDataTypes`); the converter is shared by workers, so keep it thread-safe |
| `StreamingCheckpointManager` | Resume points in the MI registry: `gov:/fileStreamingCheckpoints/{inbound}/<crc32>.json` (+ `.bak`) |
| `ReplyFileWriter` | Appends each mediated result to `ReplyFileURI` (JSON Lines for JSON results) |
| `filter/SizeCheckFilter` | Holds back files still being written, by comparing size and mtime across `CheckSizeInterval` |
| `src/main/resources/uischema.json` | Parameter definitions shown in the MI tooling; add new parameters here too |
| `docs/` | User-facing reference (`file-streaming-inbound-reference.html`), CSV types, reply file |

New parameters go in `StreamingConstants` (name and default), `VFSConfig` (parsing, falling back
to the default on a bad value), `uischema.json`, and the reference doc.

## Parallel mediation (`transport.vfs.StreamingParallelism`)

Default `1` mediates serially on the polling thread, exactly as before the feature existed. With
`N > 1`, the stream is a three-stage pipeline:

1. **Prepare (polling thread, serial):** read and tokenize the input, split out invalid records.
2. **Mediate (up to `N` workers):** build the message, `injectInbound` inline, return the result.
3. **Commit (polling thread, file order):** counters, parse/mediation error sidecars, reply-file
   append, checkpoint.

`OrderedWindow` holds at most `2N` uncommitted units (`STREAMING_WINDOW_PER_WORKER`). When it is
full, the polling thread waits on the oldest unit only.

Invariants to keep when changing this code:

- **Workers only mediate.** Anything that touches `FailedRecordCollector`, `ReplyFileWriter`,
  `Progress` or the checkpoint belongs in the commit callback. Those classes are not thread-safe,
  and they rely on running in file order.
- **Commits are contiguous,** so "last committed record" is always a safe resume point. On EOF,
  shutdown and non-recoverable errors, `drain()` the window before deciding the file's outcome.
- **Parallelism 1 must stay byte-for-byte serial:** window size 1, task run inline, committed
  immediately. `OrderedWindowTest` checks this.
- **Mediation must finish inside the injection:** success is read from the `ERROR_CODE` transport
  header right after `injectInbound` returns. A non-blocking `<call>` breaks this, both serially
  and in parallel, so sequences should use `blocking="true"`.
- **Replay after a crash** is up to `2N` in-flight units plus `CheckpointInterval − 1`. A graceful
  shutdown drains in-flight units first, so nothing is replayed.

## Performance notes (measured, M1 Pro, 3 GB / 25.8M-row CSV, chunk 2000)

- **Know the bottleneck before tuning.** Sample with `jstack` during a run and see whether workers
  are idle (reader-bound) or the reader waits in `OrderedWindow.commitOldest` (worker-bound).
- **Reader floor:** ~35 s per 3 GB on JDK 8, which is commons-csv tokenizing alone. JSON building
  was moved off the reader into `CsvChunkPayload`, whose text output is verified byte-identical to
  the old Gson tree.
- **CPU-bound sequences:** use parallelism ≈ fast cores − 1 (one core for the reader). Beyond
  that, efficiency cores or hyper-threads and GC make it slower. On a coordinated (clustered) node,
  leave more headroom: a saturated CPU starved the RDBMS coordination heartbeat, MI paused the task
  mid-file, and the run resumed from its checkpoint about a minute later.
- **I/O-bound sequences** (backend calls): the speedup is close to `N` (8× measured at N=8 with a
  200 ms backend). Size `N` to what the backend can handle, not to the core count.
- **Heap:** with a 1 GB heap and many in-flight chunks, ParallelGC spent ~45% of wall time in
  pauses. Give large-chunk workloads more heap, or reduce parallelism or chunk size.

## Testing on a real server

Use a disposable inbound and folders, not a shared demo, and expect hot redeploys of inbound XML to
interrupt an in-flight file (it checkpoints and resumes). To check ordering and crash safety, use a
slow backend that echoes the chunk number, a blocking `<call>`, a reply file, then `kill -9`
mid-file and restart. Expect no gaps, and duplicates only within the replay bound above.
