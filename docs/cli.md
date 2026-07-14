# CLI

Install with the `cli` extra: `pip install dbkit-sql[cli]`. Every command takes a YAML config file
(the same shape as `DbkitConfig.from_yaml`, §30). All output redacts secrets; commands that
touch the network report classified errors cleanly instead of raw tracebacks, and exit non-zero
on failure.

```bash
dbkit --help
```

## Commands that never touch the network

```bash
dbkit config-validate config.yaml          # validate + print a secret-redacted summary
dbkit connection-budget config.yaml --replicas 5   # projected cluster-wide connection count
dbkit engines config.yaml                  # list configured database targets
dbkit query-list                           # queries registered in this process's default registry
```

## Commands that connect to the database(s)

```bash
dbkit check config.yaml                    # validate config, then a full readiness check
dbkit health config.yaml [--database app]  # readiness check, optionally scoped to one database
dbkit pools config.yaml                    # warm a connection, then print pool status
```

Example:

```console
$ dbkit check config.yaml
configuration OK: 1 database(s), environment='production'
  app.primary: OK
all required databases are ready

$ dbkit pools config.yaml
production:app:default:primary:psycopg: size=10 checked_out=0 overflow=0 utilization=0% created=1 closed=0 invalidations=0
```

## Exit codes

- `0` — success.
- `1` — configuration error, missing file, a required database failing its readiness check,
  or a classified `DatabaseError` from a network command.
