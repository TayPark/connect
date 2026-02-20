# Adding Schema Metadata to a CDC Input

This guide describes how to add `schema` metadata to CDC (change data capture) messages in a Redpanda Connect input, based on the MySQL and PostgreSQL implementations.

---

## Background

CDC inputs emit messages for two distinct phases:

- **Snapshot** — an initial bulk-read of existing table rows before streaming begins
- **CDC stream** — an ongoing stream of change events (insert/update/delete) from the database's replication log

Both phases should attach a `schema` metadata key to every outgoing message. That metadata describes the table's column structure in a normalised format that downstream processors and sinks can introspect without knowing the source database type.

---

## The Schema Wire Format

Schema metadata is a nested `map[string]any` produced by `schema.Common.ToAny()` from `github.com/redpanda-data/benthos/v4/public/schema`. It looks like:

```json
{
  "name": "orders",
  "type": "OBJECT",
  "optional": false,
  "children": [
    { "name": "id",         "type": "INT64",     "optional": true },
    { "name": "customer",   "type": "STRING",    "optional": true },
    { "name": "total",      "type": "FLOAT64",   "optional": true },
    { "name": "created_at", "type": "TIMESTAMP", "optional": true },
    { "name": "tags",       "type": "ARRAY",     "optional": true,
      "children": [{ "name": "element", "type": "STRING", "optional": false }] }
  ]
}
```

### Canonical type names

| `schema.CommonType` | String value  | Use for                                                  |
|---------------------|---------------|----------------------------------------------------------|
| `schema.Object`     | `"OBJECT"`    | Table root                                               |
| `schema.String`     | `"STRING"`    | TEXT, VARCHAR, CHAR, JSON, DECIMAL, ENUM, DATE, TIME, UUID |
| `schema.Int32`      | `"INT32"`     | SMALLINT, INT (when 32-bit is certain)                   |
| `schema.Int64`      | `"INT64"`     | BIGINT, general integers                                 |
| `schema.Float32`    | `"FLOAT32"`   | REAL / FLOAT4                                            |
| `schema.Float64`    | `"FLOAT64"`   | DOUBLE PRECISION / FLOAT8                                |
| `schema.Boolean`    | `"BOOLEAN"`   | BOOL / BOOLEAN                                           |
| `schema.Timestamp`  | `"TIMESTAMP"` | TIMESTAMP, TIMESTAMPTZ, DATETIME                         |
| `schema.ByteArray`  | `"BYTE_ARRAY"`| BYTEA, BLOB, BINARY                                      |
| `schema.Array`      | `"ARRAY"`     | SET, array types — add a `children` entry for element type |

When in doubt, fall back to `schema.String`. Unknown types are preferable as strings over hard errors that break the stream.

---

## Implementation Steps

### 1. Write a type-mapping function

Create a `schema.go` file (or equivalent) in your implementation package. Write one function that maps a single native column descriptor to `schema.Common`, and one that converts a whole table:

```go
// columnToCommon converts one native column to the common schema type.
// Return an error only for genuinely unrecoverable conditions.
func columnToCommon(col NativeColumn) (schema.Common, error) {
    var t schema.CommonType
    switch col.NativeType {
    case NativeTypeInt:
        t = schema.Int64
    case NativeTypeFloat:
        t = schema.Float64
    // ... other mappings ...
    default:
        t = schema.String // safe fallback
    }
    return schema.Common{
        Name:     col.Name,
        Type:     t,
        Optional: true, // DB columns are nullable unless proven otherwise
    }, nil
}

// tableToSchema converts a full native table descriptor to schema.Common.
func tableToSchema(tbl NativeTable) (*schema.Common, error) {
    children := make([]schema.Common, 0, len(tbl.Columns))
    for _, col := range tbl.Columns {
        c, err := columnToCommon(col)
        if err != nil {
            return nil, fmt.Errorf("column %s: %w", col.Name, err)
        }
        children = append(children, c)
    }
    return &schema.Common{
        Name:     tbl.Name,
        Type:     schema.Object,
        Optional: false,
        Children: children,
    }, nil
}
```

### 2. Add a schema cache to the input struct

```go
type myStreamInput struct {
    // ...
    tableSchemas   map[string]any  // table name -> schema.Common.ToAny()
    tableSchemasMu sync.RWMutex
}
```

Provide helpers:

```go
// getTableSchema returns the cached schema for tbl, extracting and caching it if absent.
func (i *myStreamInput) getTableSchema(tbl NativeTable) (any, error) {
    i.tableSchemasMu.RLock()
    if cached, ok := i.tableSchemas[tbl.Name]; ok {
        i.tableSchemasMu.RUnlock()
        return cached, nil
    }
    i.tableSchemasMu.RUnlock()

    common, err := tableToSchema(tbl)
    if err != nil {
        return nil, err
    }
    serialised := common.ToAny()

    i.tableSchemasMu.Lock()
    i.tableSchemas[tbl.Name] = serialised
    i.tableSchemasMu.Unlock()
    return serialised, nil
}

// getOrExtractTableSchemaByName returns the cached schema by name, or nil if not yet cached.
func (i *myStreamInput) getOrExtractTableSchemaByName(name string) any {
    i.tableSchemasMu.RLock()
    defer i.tableSchemasMu.RUnlock()
    return i.tableSchemas[name]
}

// invalidateTableSchema removes a table's cached schema (call on DDL events).
func (i *myStreamInput) invalidateTableSchema(name string) {
    i.tableSchemasMu.Lock()
    delete(i.tableSchemas, name)
    i.tableSchemasMu.Unlock()
}
```

### 3. Populate the cache during snapshot

The snapshot phase runs before the CDC stream starts. At the top of the per-table loop in your snapshot function, pre-warm the cache:

```go
func (i *myStreamInput) readSnapshot(ctx context.Context) error {
    for _, table := range i.tables {
        // Pre-populate schema cache so snapshot messages carry schema metadata.
        if tbl, err := i.fetchNativeTable(table); err == nil {
            if _, err := i.getTableSchema(tbl); err != nil {
                i.logger.Warnf("Failed to pre-populate schema for table %s during snapshot: %v", table, err)
            }
        } else {
            i.logger.Warnf("Failed to fetch schema for table %s during snapshot: %v", table, err)
        }

        // ... proceed with reading rows ...
    }
}
```

Treat schema pre-population failures as **warnings, not errors**. A schema fetch failure should not abort the snapshot — the message will simply lack schema metadata for that table.

### 4. Populate the cache during CDC row events

In your CDC row event handler, call `getTableSchema` before (or alongside) constructing the message. This also handles tables that are seen for the first time mid-stream:

```go
func (i *myStreamInput) onRowEvent(event NativeRowEvent) error {
    if _, err := i.getTableSchema(event.Table); err != nil {
        return fmt.Errorf("failed to extract schema for table %s: %w", event.Table.Name, err)
    }
    // ... build and enqueue the message ...
}
```

### 5. Attach the schema when building the outgoing message

In the function that converts queued events to `service.Message` objects:

```go
msg := service.NewMessage(nil)
msg.SetStructuredMut(rowData)
msg.MetaSetMut("table", tableName)
// ... other metadata ...

if s := i.getOrExtractTableSchemaByName(tableName); s != nil {
    msg.MetaSetMut("schema", s)
}
```

### 6. Invalidate the cache on DDL events

If the database's replication protocol reports DDL changes (ALTER TABLE, RENAME TABLE, etc.), invalidate the affected table so the next row event re-extracts fresh schema:

```go
func (i *myStreamInput) onDDLEvent(schemaName, tableName string) error {
    if !i.isTrackedTable(tableName) {
        return nil
    }
    i.invalidateTableSchema(tableName)
    i.logger.Infof("Schema cache invalidated for %s.%s due to DDL change", schemaName, tableName)
    return nil
}
```

---

## Approach Comparison: Pre-populate vs. SQL ColumnType

There are two distinct ways to derive the schema for **snapshot messages**.

### Option A — Pre-populate from the native schema API (MySQL approach)

Before reading rows, call the database's schema introspection API (e.g. `canal.GetTable`) to get a fully-typed native descriptor, then run it through the same `tableToSchema` function used by CDC events.

**Pros:**
- Snapshot and CDC messages use the identical type-mapping path, so there are no type inconsistencies between phases.
- Works even when the query returns no rows (empty table snapshot still emits correct schema).

**Cons:**
- Requires an extra round-trip to the database before the first row is read.
- Depends on the schema API being available (not all DB client libraries expose this).

### Option B — Derive from `sql.ColumnType` (PostgreSQL approach)

After executing the snapshot SELECT, call `rows.ColumnTypes()` and map each `sql.ColumnType.DatabaseTypeName()` string to a common type using the same function used for CDC events.

**Pros:**
- No extra network round-trip; the information comes back with the query.
- Works with any `database/sql` driver.

**Cons:**
- Type name strings from `DatabaseTypeName()` may differ between drivers and DB versions.
- If the table is empty, there are no rows to call `ColumnTypes()` on (though `rows.ColumnTypes()` works even before the first `rows.Next()` call, so this is not actually a problem in practice).

**Rule of thumb:** use Option A when the database client library exposes rich type constants (like go-mysql's `TYPE_*` constants); use Option B when you only have `database/sql`.

---

## Potential Issues

### Schema not attached to snapshot messages

**Symptom:** CDC messages have `schema` metadata; snapshot messages do not.

**Cause:** The schema cache is only populated by CDC row event handlers. Since snapshot runs before CDC starts, the cache is empty when snapshot messages are built.

**Fix:** Pre-populate the cache at the start of each table's snapshot loop (Step 3 above).

---

### Stale schema after DDL change

**Symptom:** Post-ALTER messages still reflect the old column list.

**Cause:** The cache was populated before the ALTER and never invalidated.

**Fix:** Hook into the replication protocol's DDL notification mechanism and call `invalidateTableSchema` (Step 6 above). For PostgreSQL, the logical replication protocol automatically re-sends a `RelationMessage` whenever a table's structure changes; for MySQL, the canal library fires `OnTableChanged`.

---

### Type mismatch between snapshot and CDC messages

**Symptom:** A column reports `"INT32"` in snapshot messages but `"INT64"` in CDC messages (or similar inconsistencies).

**Cause:** Snapshot uses `sql.ColumnType.DatabaseTypeName()` while CDC uses the native type constants, and the two code paths map the same underlying type differently.

**Fix:** Ensure both paths call the same `columnToCommon` function. If you must use `DatabaseTypeName()` for snapshot, normalise the string using the same switch statement you use for CDC types, or use Option A (pre-populate from native schema API) to share a single code path.

---

### Race condition on schema cache

**Symptom:** Intermittent nil-pointer panics or incorrect schema under concurrent reads.

**Cause:** The cache `map[string]any` is written by multiple goroutines without synchronisation.

**Fix:** Protect all reads and writes with `sync.RWMutex` (use `RLock/RUnlock` for reads, `Lock/Unlock` for writes). See the helper pattern in Step 2 above.

---

### Schema fetch failure aborts snapshot

**Symptom:** Snapshot fails for all tables if a single `fetchNativeTable` call returns an error.

**Cause:** Schema pre-population error is treated as fatal.

**Fix:** Log a warning and continue. The snapshot data is still valuable even without schema metadata.

---

### Empty table has no schema

**Symptom:** An empty table produces no snapshot messages and therefore never warms the cache; the first CDC insert message also lacks schema.

**Cause (Option B only):** When using `sql.ColumnType`, there are no rows to derive column types from. This is a theoretical concern — `rows.ColumnTypes()` returns column metadata regardless of whether any rows were returned — but is avoided entirely by Option A.

**Cause (both options):** If you only call `getTableSchema` inside a row loop and the table is empty, the CDC handler may not be called until the first INSERT, which can be after a consumer has already started reading.

**Fix:** Always pre-populate during snapshot (Step 3), not only when rows are present.

---

### DDL notification not fired for all DDL types

**Symptom:** Renaming a column or changing a column's type is not reflected in subsequent messages.

**Cause:** Some replication protocols only fire DDL notifications for structural changes they consider significant.

**Fix:** In integration tests, cover ALTER TABLE ADD COLUMN, ALTER TABLE MODIFY COLUMN, and RENAME COLUMN. If the protocol cannot detect a specific DDL type, document the limitation.

---

## Testing the Functionality

### Unit tests

Test the type-mapping function in isolation with a table that covers every native type:

```go
func TestColumnToCommon(t *testing.T) {
    cases := []struct {
        nativeType NativeType
        expected   schema.CommonType
    }{
        {NativeTypeInt,   schema.Int64},
        {NativeTypeFloat, schema.Float64},
        {NativeTypeText,  schema.String},
        // ...
    }
    for _, tc := range cases {
        got, err := columnToCommon(NativeColumn{NativeType: tc.nativeType, Name: "col"})
        require.NoError(t, err)
        assert.Equal(t, tc.expected, got.Type)
    }
}
```

Test that `tableToSchema` produces a correctly structured `schema.Common`:

```go
func TestTableToSchema(t *testing.T) {
    tbl := NativeTable{Name: "users", Columns: []NativeColumn{...}}
    s, err := tableToSchema(tbl)
    require.NoError(t, err)
    assert.Equal(t, schema.Object, s.Type)
    assert.Len(t, s.Children, len(tbl.Columns))
}
```

Test cache helpers directly:

```go
func TestSchemaCache(t *testing.T) {
    input := &myStreamInput{tableSchemas: map[string]any{}}
    // First call extracts and caches
    s1, err := input.getTableSchema(fakeTable)
    require.NoError(t, err)
    // Second call hits cache (same pointer)
    s2, _ := input.getTableSchema(fakeTable)
    assert.Equal(t, s1, s2)
    // Invalidate removes it
    input.invalidateTableSchema(fakeTable.Name)
    assert.Nil(t, input.getOrExtractTableSchemaByName(fakeTable.Name))
}
```

### Integration test: snapshot schema

The integration test must verify schema is present on snapshot messages (not just CDC messages). Use `require` (not just `t.Log`) so the test actually fails if schema is absent:

```go
// Check snapshot messages
for i, msg := range snapshotMessages {
    require.True(t, msg.hasSchema, "snapshot message %d must have schema metadata", i)
    require.NotNil(t, msg.schema)

    children, ok := msg.schema["children"].([]any)
    require.True(t, ok)

    fieldSchemas := make(map[string]map[string]any)
    for _, child := range children {
        m := child.(map[string]any)
        fieldSchemas[m["name"].(string)] = m
    }

    assert.Equal(t, "INT64",     fieldSchemas["id"]["type"])
    assert.Equal(t, "STRING",    fieldSchemas["name"]["type"])
    assert.Equal(t, "TIMESTAMP", fieldSchemas["created_at"]["type"])
    // ... all columns ...
}
```

### Integration test: CDC schema

The CDC schema test follows the same pattern as above but for insert/update/delete messages. Both should use `require.True(t, msg.hasSchema)` — not an optional log.

### Integration test: snapshot and CDC schema consistency

Assert that the schema on snapshot message 0 is structurally identical to the schema on the first CDC insert message. This catches type-mapping divergence between the two code paths:

```go
assert.Equal(t, snapshotMessages[0].schema, cdcMessages[0].schema,
    "snapshot and CDC schema must be identical")
```

### Integration test: DDL invalidation

```
1. Start CDC input.
2. Consume initial snapshot messages; assert schema has N columns.
3. Execute ALTER TABLE ADD COLUMN.
4. Insert a new row.
5. Consume the CDC insert message; assert schema now has N+1 columns.
6. Assert the new column is present with the correct type.
```

### Checklist for verifying a complete implementation

- [ ] Snapshot messages carry `schema` metadata for every tracked table
- [ ] CDC insert messages carry `schema` metadata
- [ ] CDC update and delete messages carry `schema` metadata
- [ ] Schema on snapshot messages is structurally identical to schema on CDC messages for the same table
- [ ] After ALTER TABLE, subsequent CDC messages reflect the new column set
- [ ] Empty tables still produce correct schema on the first CDC event
- [ ] Schema fetch failure during snapshot logs a warning but does not abort the snapshot
- [ ] All columns use the correct canonical type strings (`"INT64"`, `"STRING"`, etc.)
- [ ] Array/set columns include a `children` entry describing the element type
