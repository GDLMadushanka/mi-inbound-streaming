# Streaming reply file

When `transport.vfs.ReplyFileURI` is set, the mediated result of every chunk/record is **appended**
to a file in that folder. One streamed source file produces many messages, so append is the only
coherent semantic — overwriting would leave just the last chunk. There is no append/overwrite
switch.

```xml
<parameter name="transport.vfs.ReplyFileURI">/data/replies</parameter>
<parameter name="transport.vfs.ReplyFileName">orders-out.jsonl</parameter>  <!-- optional -->
```

Leave `ReplyFileURI` unset and no reply file is written at all — the writer is never constructed.

## What gets written

The payload type is detected per message and decides both the format and the default file name.

| payload | how it is written | default file name |
|---|---|---|
| JSON (`JsonUtil.hasAJsonPayload`) | `JsonUtil.writeAsJson`, one document per line — JSON Lines | `response.jsonl` |
| anything else | the Axis2 `MessageFormatter` for the message | `response.xml` |

The non-JSON path is the same code path the runtime already uses for VFS replies —
`MessageProcessorSelector.getMessageFormatter` + `BaseUtils.getOMOutputFormat` +
`formatter.writeTo(..., preserve = true)`, exactly as `VFSTransportSender.populateResponseFile`
does — so an XML reply is what a `<send>` to a VFS endpoint would have produced.

One difference from the sender: a newline is written after each record. The sender writes one
message per file and needs no separator; an appended file does, or the records run together on a
single line.

Note: on MI 4.1.0 the Synapse API is `JsonUtil.hasAJsonPayload(MessageContext)`. Later releases
add `hasJsonPayload`; it does not exist here.

JSON Lines output is re-readable by this same inbound with
`transport.vfs.StreamingInputFormat=jsonl`, so a reply file can feed a second stage.

## Rules

- **Successful records only.** A record that fails mediation goes to the mediation-error sidecar
  (`transport.vfs.StreamingMediationErrorFolder`) and is not appended, so it never appears twice.
- **Empty bodies are skipped.** In property-output mode the message starts as an empty SOAP
  envelope with the parsed record in `transport.vfs.StreamingOutputProperty`. If the sequence never
  builds a payload there is nothing to reply with, so nothing is appended and one warning is logged
  per file — not per record.
- **One handle per source file.** The reply file is opened lazily on the first append and held for
  that source file, then closed in the same `finally` as the failed-record sidecars. A source file
  that produces no reply creates no file.
- **Never fails a file.** If the destination cannot be opened, or a write fails, the writer logs
  once and goes quiet. Records already delivered to the sequence are not affected.
- **Ordering is guaranteed within a source file** because streaming always injects sequentially.

## Known limitation

Two source files processed concurrently — `transport.vfs.FileProcessCount` > 1, several inbounds
sharing one `ReplyFileURI`, or more than one node — append to the same destination through separate
handles. Writes are not coordinated across them, so records can interleave. If that matters, give
each inbound its own `ReplyFileURI`.

## Worked example

`orders.csv` streamed in `RECORD` mode with `StreamingOutputProperty=streamOut` and a sequence that
builds a JSON payload:

```xml
<payloadFactory media-type="json">
    <format>{"row":$1,"seen":"yes"}</format>
    <args><arg expression="get-property('streamOut')"/></args>
</payloadFactory>
```

produces `replies/response.jsonl`:

```
{"row":{"id":"1","customer":"Alice","amount":"120.50","currency":"USD"},"seen":"yes"}
{"row":{"id":"2","customer":"Bob","amount":"75.00","currency":"EUR"},"seen":"yes"}
...
```

A second source file appends to the same file rather than replacing it.
