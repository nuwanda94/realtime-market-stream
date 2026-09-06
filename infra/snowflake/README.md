# Snowflake setup scripts

Example warehouse objects for the optional dual-write path:

| File | Purpose |
|------|---------|
| `00_roles.sql` | App / ingest / reader roles and grants |
| `01_warehouse_database.sql` | XS auto-suspend warehouse, database, schema |
| `02_tables.sql` | `TICKS` and `BARS` (VARIANT payload + projected columns) |
| `03_stage_pipe.sql` | Internal stage + Snowpipe example (AUTO_INGEST=false) |

Placeholders look like `{{DATABASE}}` and are filled from `SNOWFLAKE_*` settings.

```bash
# Print SQL only — default, zero cloud cost
make snowflake-setup

# Execute against a real account (requires extras + credentials)
SNOWFLAKE_LOCAL_CAPTURE=false make snowflake-setup APPLY=1
```

Do not commit account identifiers or passwords. Copy `.env.example` to `.env`.
