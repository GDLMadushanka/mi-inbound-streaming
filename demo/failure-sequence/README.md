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
