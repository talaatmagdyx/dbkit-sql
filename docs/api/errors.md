# Errors

Every exception dbkit raises is a subclass of `DatabaseError`, normalized from the underlying
driver/SQLAlchemy exception via SQLSTATE-first classification (see `docs/requirements.md`
§13). Bound parameters and DSNs are never present in error messages (§29).

::: dbkit.errors
    options:
      show_submodules: true
