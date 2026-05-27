"""SQL helpers reused by galaxy ingestion DAGs."""


def sql_quote(value):
    """Return a SQL literal for simple scalar values used in demo inserts."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)
