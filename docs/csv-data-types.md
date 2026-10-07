# CSV column data types

CSV carries no type information, so by default every cell of a streamed CSV file becomes a JSON
string:

```json
{"id":"1","customer":"Alice","amount":"120.50","qty":"3","active":"true"}
```

`transport.vfs.StreamingCsvDataTypes` declares the JSON type of selected columns, so they are
emitted as numbers and booleans instead:

```json
{"id":1,"customer":"Alice","amount":120.50,"qty":3,"active":true}
```

## Configuration

```xml
<parameter name="transport.vfs.StreamingCsvDataTypes">[{"Column Name Or Index":"id","Is Column Name":"Yes","Data Type":"Number"},{"Column Name Or Index":"2","Is Column Name":"No","Data Type":"String"}]</parameter>
```

`transport.vfs.csvDataTypes` is accepted as an alias; the `StreamingCsv`-prefixed spelling matches
the other CSV parameters (`StreamingCsvDelimiter`, `StreamingCsvQuote`, `StreamingCsvHasHeader`).

The value is a JSON array of rules. It is the same shape the WSO2 CSV mediation module takes for
its `dataTypes` property, so a mapping can be moved between the two unchanged.

| field | meaning |
|---|---|
| `Column Name Or Index` | a header name, or a **1-based** column position — `"2"` is the second column |
| `Is Column Name` | `Yes` (the default when absent) if the field above is a header name, `No` if it is a position |
| `Data Type` | `String`, `Boolean`, `Integer` or `Number` — matched case-insensitively |

Columns with no rule keep the existing behaviour and stay strings, so adding this parameter only
changes the columns it names.

### Where it applies

- Only when `transport.vfs.StreamingOutputProperty` is set. Without it the inbound emits the
  original CSV text as the message body, which has no JSON types to assign.
- In both `CHUNK` and `RECORD` modes, and whether rows are emitted as JSON objects
  (`transport.vfs.StreamingAddHeadersToEachResult=true`) or as JSON arrays.
- Name-based rules need `transport.vfs.StreamingCsvHasHeader=true`. Without a header row there are
  no names to match, so use 1-based positions with `"Is Column Name":"No"`.

## Conversion rules

| type | conversion |
|---|---|
| `String` | left as-is |
| `Integer` | parsed as a 64-bit integer |
| `Number` | parsed as an exact decimal — scale is preserved, so `120.50` stays `120.50` |
| `Boolean` | `true` / `false`, case-insensitive; nothing else |

Leading and trailing whitespace around a typed value is ignored, so ` 8 ` converts.

Two deliberate differences from the CSV mediation module, both in the direction of not losing data:

- **`Integer` uses 64-bit parsing**, not `int`. Ids and account numbers routinely exceed the 32-bit
  range, and JSON draws no int/long distinction.
- **`Number` uses exact decimal parsing**, not `double`. A monetary `120.50` stays `120.50` rather
  than becoming `120.5`, and long decimals are not rounded to a double's precision.

`Boolean` also differs: `Boolean.parseBoolean` maps every unrecognised value to `false`, which
would silently turn a `y`/`n` or `1`/`0` column into all-`false`. Here anything that is not
`true` or `false` is treated as unconvertible and kept as a string instead.

## What happens when something does not fit

Nothing here can fail a file. Every problem degrades to the string the inbound would have produced
anyway, and is logged.

| situation | result |
|---|---|
| the parameter is absent or blank | no typing; every cell is a string |
| the value is not a JSON array | logged as an error at inbound init; no typing |
| one rule is malformed (missing column, unknown type, non-numeric index, index `< 1`) | that rule is logged and skipped; the others still apply |
| a named column is not in the file's header | logged once per file; that column stays a string |
| an index is past the last column | logged once per file; ignored |
| a cell does not parse as its declared type (`"N/A"` in a `Number` column) | that cell is emitted as a string, **logged once per column per file** |
| a cell is empty | emitted as `""`, the same as without this parameter |

The once-per-column cap on conversion warnings matters: a streamed file can hold millions of rows,
and one bad column would otherwise write one log line per row.

Configuration errors are reported when the inbound is initialised, not per polled file, so a typo
surfaces at deployment rather than on every poll.

## Worked example

`orders-typed.csv`:

```csv
id,customer,amount,qty,active,note
1,Alice,120.50,3,true,first
2,Bob,75.00,1,false,
3,Carol,240.25,12,TRUE,"has, comma"
4,Dave,N/A,7,maybe,bad amount
5,Erin,9999999999.99,2,false,big
```

with

```xml
<parameter name="transport.vfs.StreamingCsvDataTypes">[{"Column Name Or Index":"id","Is Column Name":"Yes","Data Type":"Integer"},{"Column Name Or Index":"amount","Is Column Name":"Yes","Data Type":"Number"},{"Column Name Or Index":"4","Is Column Name":"No","Data Type":"Integer"},{"Column Name Or Index":"active","Is Column Name":"Yes","Data Type":"Boolean"}]</parameter>
```

produces (chunk size 3):

```json
[{"id":1,"customer":"Alice","amount":120.50,"qty":3,"active":true,"note":"first"},
 {"id":2,"customer":"Bob","amount":75.00,"qty":1,"active":false,"note":""},
 {"id":3,"customer":"Carol","amount":240.25,"qty":12,"active":true,"note":"has, comma"}]
[{"id":4,"customer":"Dave","amount":"N/A","qty":7,"active":"maybe","note":"bad amount"},
 {"id":5,"customer":"Erin","amount":9999999999.99,"qty":2,"active":false,"note":"big"}]
```

Row 4 shows the fallback: `amount` and `active` hold values that are not a number and not a
boolean, so those two cells stay strings while the rest of the row is still typed. The log carries
one warning per affected column:

```
WARN {CsvDataTypes} - CSV data types: column 3 is declared as NUMBER but holds 'N/A', which cannot
be converted. ... This is reported once per column per file.
```
