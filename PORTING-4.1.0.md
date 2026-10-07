# Porting the streaming File inbound to WSO2 MI 4.1.0

This repo is the MI 4.1.0 port of `mi-inbound-file` (branch `streaming`). The source is a
one-to-one copy of that inbound with the changes below; anything not listed here is unchanged,
so upstream fixes can be diffed across with `diff -r` after applying the package rename.

## Package rename

| upstream (`mi-inbound-file`)                   | here                                          |
|------------------------------------------------|-----------------------------------------------|
| `org.wso2.carbon.inbound.vfs`                   | `org.wso2.carbon.inbound.streaming`           |
| `org.wso2.carbon.inbound.vfs.streaming`         | `org.wso2.carbon.inbound.streaming`           |
| `org.wso2.carbon.inbound.vfs.streaming.{csv,json,jsonl,text}` | `org.wso2.carbon.inbound.streaming.{csv,json,jsonl,text}` |
| `org.wso2.carbon.inbound.vfs.{filter,lock,processor}` | `org.wso2.carbon.inbound.streaming.{filter,lock,processor}` |

`vfs` and `vfs.streaming` collapse into one package (no class names collide). The inbound class to
put in an `inboundEndpoint` is therefore:

```
class="org.wso2.carbon.inbound.streaming.VFSConsumer"
```

## Runtime differences that forced code changes

### 1. commons-vfs is not package-relocated on 4.1.0

MI 4.5+ ships commons-vfs 2.10.x relocated to `org.wso2.org.apache.commons.vfs2`. MI 4.1.0 ships
2.2.0-wso2v13.20 (`wso2/components/plugins/commons-vfs2_2.2.0.wso2v13_20.jar`), whose classes are
in plain `org.apache.commons.vfs2`. Only the Maven *groupId* keeps the `org.wso2...` prefix. Every
VFS import was rewritten accordingly.

The version matters: the `smb2` provider — including `Smb2FileSystemConfigBuilder`, which
`Utils.attachFileSystemOptions` configures — lives in **commons-vfs2-sandbox** on the 2.2 line and
only from the `.20` update onward. Plain `2.2.0-wso2v13` does not compile.

### 2. No message variables

MI 4.1.0's Synapse has no `MessageContext.setVariable`. The parsed streaming output is placed in a
Synapse **property** as a JSON string instead:

- parameter `transport.vfs.StreamingOutputProperty` (the old `transport.vfs.StreamingOutputVariable`
  is still accepted as an alias, so existing configurations keep working);
- `VFSConfig.getStreamingOutputProperty()` / `isStreamingAddOutputToProperty()`;
- read it in a sequence with `get-property('<name>')`.

The value is `JsonElement.toString()` — a JSON object for `RECORD` mode, a JSON array for `CHUNK`
mode. It is deliberately **not** `getAsString()`: that method is only defined for a single JSON
primitive and throws for both of those shapes.

### 3. `GenericPollingConsumer` is smaller on 4.1.0

- **No cron constructor.** `GenericProcessor` only ever passes a `long` scan interval, so the
  `String cronExpression` constructor is not ported. Use `interval`; `cronExpression` is not
  supported on this runtime.
- **No `pause()`.** `GenericTask.notifyLocalTaskPause()` maps onto `destroy()`, which already calls
  `close()`. `VFSConsumer.pause()` is kept but is no longer an `@Override`.
- **`resume()` only exists in the update stream.** Hence the dependency is pinned to
  `org.wso2.ei:org.wso2.micro.integrator.inbound.endpoint:4.1.0.142`, not base `4.1.0`.

### 4. `FileSystemManager` is not `Closeable` in commons-vfs 2.2

`close()` is only on `DefaultFileSystemManager`, so `VFSConsumer.close()` casts to the
`StandardFileSystemManager` it created.

### 5. Gson: `JsonParser.parseString` is unsafe here

`org.wso2.micro.integrator.inbound.endpoint` embeds a pre-2.8.6 Gson that has no static
`parseString(String)`, and that copy can win over the `com.google.gson 2.9.0` bundle. `Utils`
uses the instance method `new JsonParser().parse(...)`, which exists in both.

### 6. `org.jetbrains.annotations.NotNull`

Not on the MI 4.1.0 classpath; the single `@NotNull` in `CSVStreamingProcessor` was dropped.

## Dependency pinning

Everything in `<properties>` is pinned to what MI 4.1.0 actually ships, because several of these
lose to older versions dragged in transitively by synapse/carbon:

| artifact | version | why pinned |
|---|---|---|
| `org.wso2.ei:org.wso2.micro.integrator.inbound.endpoint` | `4.1.0.142` | base `4.1.0` has no `resume()` |
| `org.wso2.org.apache.commons:commons-vfs2{,-sandbox}` | `2.2.0-wso2v13.20` | `.20` carries the smb2 config builder |
| `org.apache.synapse:synapse-{core,commons}` | `2.1.7-wso2v271` | matches the shipped bundles |
| `com.hierynomus.wso2:smbj` / `asn-one` | `0.11.3.wso2v1` / `0.6.0.wso2v1` | matches the shipped bundles |
| `com.google.code.gson:gson` | `2.9.0` (provided) | transitive default is far older |
| `com.fasterxml.jackson.core:jackson-databind` / `-core` | `2.21.5` (provided) | — |
| `com.fasterxml.jackson.core:jackson-annotations` | `2.21` (provided) | — |
| `org.apache.commons:commons-csv` | `1.14.1` (compile) | **not** shipped by MI 4.1.0 |

`jackson-core` and `jackson-annotations` are declared explicitly rather than inherited: synapse and
carbon pull 2.13.1 of both, and Maven's nearest-wins pairs them with databind 2.21.5, whose static
initialisation then fails with `NoClassDefFoundError: Could not initialize class ObjectMapper`.

## Build

```
mvn -Dmaven.repo.local=$HOME/m2_temp3 clean install -Dmaven.test.skip=true
```

Produces `target/mi-inbound-streaming-1.0.0.jar` and a `.zip` bundle
(jar + `uischema.json` + `connector.xml` + `descriptor.yml`).

## Deploy to MI 4.1.0

1. `cp target/mi-inbound-streaming-1.0.0.jar <MI_HOME>/lib/`
2. `cp <commons-csv-1.14.1.jar> <MI_HOME>/lib/` — MI 4.1.0 does not ship commons-csv.
3. Delete any stale `<MI_HOME>/dropins/mi_inbound_streaming_*.jar` so the server regenerates the
   OSGi bundle from `lib/`, then restart.

### Minimal inbound endpoint

```xml
<inboundEndpoint xmlns="http://ws.apache.org/ns/synapse"
                 name="csvStreamInbound"
                 class="org.wso2.carbon.inbound.streaming.VFSConsumer"
                 sequence="streamTestSeq" onError="streamFaultSeq" suspend="false">
    <parameters xmlns="http://ws.apache.org/ns/synapse">
        <parameter name="scheduleType">Polling</parameter>
        <parameter name="interval">5000</parameter>
        <parameter name="sequential">true</parameter>
        <parameter name="coordination">true</parameter>
        <parameter name="transport.vfs.FileURI">/data/in</parameter>
        <parameter name="transport.vfs.MoveAfterProcess">/data/out</parameter>
        <parameter name="transport.vfs.MoveAfterFailure">/data/fail</parameter>
        <parameter name="transport.vfs.ActionAfterProcess">MOVE</parameter>
        <parameter name="transport.vfs.ActionAfterFailure">MOVE</parameter>
        <parameter name="transport.vfs.FileNamePattern">.*\.csv</parameter>
        <parameter name="transport.vfs.Locking">enable</parameter>
        <parameter name="transport.vfs.Streaming">true</parameter>
        <parameter name="transport.vfs.StreamingMode">CHUNK</parameter>
        <parameter name="transport.vfs.StreamingInputFormat">csv</parameter>
        <parameter name="transport.vfs.StreamingChunkSize">3</parameter>
        <parameter name="transport.vfs.StreamingCsvHasHeader">true</parameter>
        <parameter name="transport.vfs.StreamingOutputProperty">streamOut</parameter>
    </parameters>
</inboundEndpoint>
```

Note `scheduleType`/`interval` and no `protocol` attribute — MI 4.1.0 rejects `protocol="polling"`
for a class-based inbound (`No enum constant ... Protocols.polling`).

## Checkpointing (restored)

The upstream resume point is part of this inbound again. Every
`transport.vfs.StreamingCheckpointInterval` confirmed units, and once more at a graceful shutdown,
the inbound writes `gov:/fileStreamingCheckpoints/<inbound>/<crc32 of the file URI>.json` with the
record and chunk counts reached so far. The previous copy is kept alongside as `.bak`. On the next
poll of the same file the checkpoint is read back and streaming resumes from that record; the
checkpoint is deleted when the file completes or fails as a whole.

The source URI is **not** stored. The resource name is a CRC32 of the URI (it only has to be stable
and collision-free per inbound) and the body carries a SHA-256 of it, because an sftp or smb URI
carries credentials and the registry is a shared mount in a cluster.

### File identity: content, not timestamp

A checkpoint is only used if the file still matches the fingerprint recorded with it. That
fingerprint is **byte count + CRC32 of the leading 64 KB + the hash budget and algorithm**. The
file's modification time is recorded and printed in diagnostics but is deliberately **not** compared.

Comparing it was a real defect. Two nodes of the same cluster reported the same local file's mtime
as `1791263539594` and `1791263539000` — one instant, the second truncated to a whole second,
because the precision depends on the file system and on how the `FileObject` was resolved. The
second node therefore judged the file changed, discarded a checkpoint the first had written seconds
earlier, and re-read the whole file from record 1. The same comparison also rejected a file that had
been moved away and copied back with identical bytes.

### Resume decisions are logged

`StreamingCheckpointManager.loadResumable()` returns `null` for three different reasons, and they
used to be indistinguishable in the log. Each now names itself:

- `No streaming checkpoint at <path> ...` — nothing to resume from (INFO, the ordinary case).
- `Ignoring the streaming checkpoint at <path> ... because <reason>` — one was found and refused
  (WARN). The reason names the differing fields, e.g. `the file changed since the checkpoint was
  written (size 42->1073749726 ...)` or `the inbound's streaming configuration changed`.
- `The primary streaming checkpoint ... was unreadable; resuming from the backup copy.` (WARN)

### Shutdown

`VFSConsumer` registers a JVM shutdown hook that sets the cancel flag so the stream stops at the
next record boundary, writes its checkpoint and releases the `.lock` before the file system manager
closes. Without the hook the server waited out the full in-flight poll; with it a 1 GB file in
flight stops in about two seconds.

### Completion callbacks

`transport.vfs.StreamingFileCompleteSequence` and `...FileFailureSequence` fire once per file, after
the last unit, with `STREAMING_FILE_NAME`, `STREAMING_FILE_URI`, `STREAMING_STATUS`,
`STREAMING_PROCESSED`, `STREAMING_PARSE_FAILED` and `STREAMING_MEDIATION_FAILED` set. Neither fires
on a shutdown interruption: the file is paused, not finished.

## Beyond the port

- **CSV column data types** (`transport.vfs.StreamingCsvDataTypes`, alias
  `transport.vfs.csvDataTypes`) — declare the JSON type of selected CSV columns so they stop being
  emitted as strings. See [docs/csv-data-types.md](docs/csv-data-types.md).
- **Streaming reply file** (`transport.vfs.ReplyFileURI`) — the inbound now writes the mediated
  result of each chunk/record itself, appending to one file per source file, instead of only
  stamping reply headers for the VFS sender. Append is forced, not configurable. See
  [docs/reply-file.md](docs/reply-file.md).

## Still open

- `StreamInjectHandler.FailedRecordCollector.fileCompleted()` / `fileFailed()` are empty hooks
  carried over from the upstream working tree, where per-file metrics were being started
  (`docs/grafana/wso2-inbound-endpoint-streaming.json`). They are no-ops here.
- `.connector-store/meta.json` still carries the operation/parameter list generated for the
  upstream release; only the coordinates, product list and tag were updated.
