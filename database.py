"""Small PostgreSQL adapter for SignalDesk's fixed SQL statements."""
import re


class Row(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return tuple(self.values())[key]
        return super().__getitem__(key)


class Cursor:
    def __init__(self, cursor, lastrowid=None):
        self.cursor, self.lastrowid = cursor, lastrowid

    def fetchone(self):
        value = self.cursor.fetchone()
        return Row(value) if value is not None else None

    def fetchall(self):
        return [Row(value) for value in self.cursor.fetchall()]

    def __iter__(self):
        for value in self.cursor:
            yield Row(value)


class PostgresConnection:
    def __init__(self, dsn):
        import psycopg
        from psycopg.rows import dict_row
        self.connection = psycopg.connect(dsn, row_factory=dict_row, connect_timeout=10)
        self.connection.execute("SET lock_timeout = '15s'")
        self.connection.execute("SET statement_timeout = '30s'")

    def execute(self, statement, parameters=()):
        # Only internal fixed SQL reaches this adapter; user values remain bound parameters.
        info = re.fullmatch(r"PRAGMA table_info\(([a-z_]+)\)", statement)
        if info:
            statement = "SELECT ordinal_position-1 AS cid,column_name AS name,data_type AS type FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=%s ORDER BY ordinal_position"
            return Cursor(self.connection.execute(statement, (info.group(1),)))
        statement = statement.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        statement = re.sub(r"\bREAL\b", "DOUBLE PRECISION", statement)
        ignore = "INSERT OR IGNORE INTO" in statement
        statement = statement.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        if ignore:
            statement += " ON CONFLICT DO NOTHING"
        statement = statement.replace("? IS NULL", "CAST(? AS TEXT) IS NULL").replace("?", "%s")
        returning_id = statement.startswith("INSERT INTO batches(")
        if returning_id:
            statement += " RETURNING id"
        cursor = self.connection.execute(statement, parameters or None)
        lastrowid = cursor.fetchone()["id"] if returning_id else None
        return Cursor(cursor, lastrowid)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()
