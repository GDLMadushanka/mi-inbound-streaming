# Testing `transport.vfs.StreamingFileFailureSequence`

A minimal, self-contained inbound that fires the failure callback on demand.

## Install

```
cp failDemoInbound.xml  <MI_HOME>/repository/deployment/server/synapse-configs/default/inbound-endpoints/
cp failDemo*Seq.xml     <MI_HOME>/repository/deployment/server/synapse-configs/default/sequences/
mkdir -p <demo>/failtest/{in,out,fault,replies}
```

Edit the four absolute paths in `failDemoInbound.xml` to match your folder, then start MI.

## What makes a file fail as a whole

The failure sequence fires only on an **unrecoverable** error, not on a bad record. There are
two sources:

1. A **non-recoverable parse error** — the stream cannot be read any further. For CSV and JSON
   the document is a single unit, so one malformed row poisons the rest of the file.
   `StreamInjectHandler.handleStreamingException()` returns `false` and the file is abandoned.
2. Any **other exception** while opening or reading the file (the outer `catch` in
   `StreamInjectHandler.stream()`) — an I/O error, a vanished file, a bad charset.

A *recoverable* per-record error is not this: it is counted in `parseFailed` /
`mediationFailed`, the record goes to the error folder, and the file carries on to completion.

## Run the failing case

```
cp broken-orders.csv <demo>/failtest/in/
```

`broken-orders.csv` line 5 is `4,"Dave" Smith,9.99,5` — a quoted token followed by bare text,
which commons-csv rejects outright.

Observed:

```
ERROR Unrecoverable streaming error at row 5. Aborting processing the file : .../broken-orders.csv
      StreamingException{message='Error reading CSV record', rowNumber=5, isRecoverable=false}
INFO  === FILE FAILED === = broken-orders.csv,
      uri = file:///.../failtest/in/broken-orders.csv, status = FAILED,
      processed = 2, parseFailed = 1, mediationFailed = 0,
      summaryBody = {"file":"broken-orders.csv","uri":"file:///...","status":"FAILED",
                     "processed":2,"parseFailed":1,"mediationFailed":0}
```

The file lands in `failtest/fault/` (`ActionAfterFailure=MOVE`), and `replies/rows.jsonl` holds
the two rows that did mediate.

## Run the passing case, for contrast

```
cp good-orders.csv <demo>/failtest/in/
```

```
INFO  Streaming completed for good-orders.csv: processed=3, parseFailed=0, mediationFailed=0
INFO  === FILE COMPLETE === = good-orders.csv, status = COMPLETED, processed = 3, parseFailed = 0
```

The file lands in `failtest/out/`.

## Two things to know about the numbers

**The row number is the file line, not the record number.** The error above says `row 5`;
that is line 5 of the file (the header is line 1), so you can open the file and go straight
to it. `STREAMING_RECORD_NUMBER` on a delivered message counts data rows instead, and does
not include the header.

**`processed` undercounts by one on a fatal abort.** `broken-orders.csv` has three valid rows
before the bad one, but the summary says `processed=2`. The CSV iterator reads one record
ahead: `next()` takes the buffered record and then calls `advanceToNextRecord()`, which is
where the fatal error is raised — so the already-parsed row 3 is discarded before it is
returned. Nothing is lost permanently, because the whole file is moved to the fault folder for
reprocessing, but do not read `processed` as "rows that were valid".

## Neither callback fires on shutdown

A graceful shutdown is not a failure. The inbound checkpoints, releases the lock and leaves the
file in place; the file is paused, not finished, so neither sequence runs.

---

# `StreamingFileFailureSequence` vs `onError`

They are different scopes and never substitute for each other.

| | `onError` (inbound attribute) | `StreamingFileFailureSequence` |
|---|---|---|
| Scope | one message | one file, at most once |
| Wired by | `seq.setErrorHandler(onErrorSeq)` in `AbstractInjectHandler` | `onFileFailure()` in `StreamInjectHandler` |
| Fires when | mediation of that message raises a fault | the file is abandoned unread |
| Applies to | the record/chunk sequence **and** the complete/failure callbacks | nothing else |
| Effect on the file | none — the loop continues | the file is already failed |

## What triggers which — verified on MI 4.1.0

| Event | `onError` | Failure seq | Complete seq | File goes to |
|---|---|---|---|---|
| Mediator in the record sequence throws | **yes**, per record | no | **yes** | `MoveAfterProcess` |
| Recoverable per-record parse error | no | no | **yes** | `MoveAfterProcess` |
| Non-recoverable parse error (malformed CSV/JSON) | no | **yes** | no | `MoveAfterFailure` |
| File cannot be opened or read | no | **yes** | no | `MoveAfterFailure` |
| Graceful shutdown mid-file | no | no | no | stays in place |
| The complete/failure sequence itself throws | **yes** | — | — | unchanged |

A mediation fault never fails the file. A whole-file failure never runs `onError` — the abort
happens in the inbound's own code, not inside a mediation.

## The trap: `onError` hides the failure from the counters

`injectToSequence` decides success from one thing only — whether the `ERROR_CODE` entry is set
in the Axis2 `TRANSPORT_HEADERS` map. It does not look at whether a fault occurred.

So an `onError` sequence that merely logs **swallows the failure**. Observed with row 2 throwing:

```
INFO  >>> onError FIRED = 2, errMsg = The script engine returned an error ...
INFO  Streaming completed for good-orders.csv: processed=3, parseFailed=0, mediationFailed=0
```

Three processed, zero failed — even though row 2 never produced a result. The record is also
absent from the reply file, so it vanishes silently.

Report the failure back by setting the header in the fault sequence:

```xml
<property name="ERROR_CODE" value="VFS_MEDIATION_FAILED" scope="transport"/>
```

With that one line, the same run gives:

```
WARN  Record 2 failed mediation; moving the record.
INFO  Streaming completed for retest.csv: processed=2, parseFailed=0, mediationFailed=1
```

and the record is routed to the `StreamingMediationErrorFolder` sidecar.

**Rule of thumb:** if you attach an `onError` sequence to a streaming inbound, either set
`ERROR_CODE` in it, or accept that `mediationFailed` will always read zero.
