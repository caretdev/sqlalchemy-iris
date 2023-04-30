import logging

from typing import Optional

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.elements import ClauseElement

from alembic.ddl import DefaultImpl
from alembic.ddl.base import ColumnNullable
from alembic.ddl.base import ColumnType
from alembic.ddl.base import ColumnName
from alembic.ddl.base import Column
from alembic.ddl.base import alter_table
from alembic.ddl.base import alter_column
from alembic.ddl.base import format_type
from alembic.ddl.base import format_column_name

from .base import IRISDDLCompiler

log = logging.getLogger(__name__)


class IRISImpl(DefaultImpl):
    __dialect__ = "iris"

    type_synonyms = DefaultImpl.type_synonyms + (
        {"BLOB", "LONGVARBINARY"},
        {"DOUBLE", "FLOAT"},
        {"DATETIME", "TIMESTAMP"},
    )

    def compare_type(self, inspector_column: Column, metadata_column: Column) -> bool:
        # Don't change type of IDENTITY column
        if (
            metadata_column.primary_key
            and metadata_column is metadata_column.table._autoincrement_column
        ):
            return False

        return super().compare_type(inspector_column, metadata_column)

    def compare_server_default(
        self,
        inspector_column: Column,
        metadata_column: Column,
        rendered_metadata_default,
        rendered_inspector_default,
    ):
        # don't do defaults for IDENTITY columns
        if (
            metadata_column.primary_key
            and metadata_column is metadata_column.table._autoincrement_column
        ):
            return False

        return super().compare_server_default(
            inspector_column,
            metadata_column,
            rendered_metadata_default,
            rendered_inspector_default,
        )

    def drop_column(
        self,
        table_name: str,
        column: Column,
        schema: Optional[str] = None,
        **kw,
    ) -> None:
        column_name = column.name
        fkeys = self.dialect.get_foreign_keys(self.connection, table_name, schema)
        fkey = [
            fkey["name"] for fkey in fkeys if column_name in fkey["constrained_columns"]
        ]
        if len(fkey) == 1:
            self._exec(_ExecDropForeignKey(table_name, fkey[0], schema))
        super().drop_column(table_name, column, schema, **kw)


class _ExecDropForeignKey(Executable, ClauseElement):
    inherit_cache = False

    def __init__(
        self, table_name: str, foreignkey_name: Column, schema: Optional[str]
    ) -> None:
        self.table_name = table_name
        self.foreignkey_name = foreignkey_name
        self.schema = schema


@compiles(_ExecDropForeignKey, "iris")
def _exec_drop_foreign_key(
    element: _ExecDropForeignKey, compiler: IRISDDLCompiler, **kw
) -> str:
    return "%s DROP FOREIGN KEY %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.foreignkey_name),
    )


@compiles(ColumnNullable, "iris")
def visit_column_nullable(
    element: ColumnNullable, compiler: IRISDDLCompiler, **kw
) -> str:
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "NULL" if element.nullable else "NOT NULL",
    )


@compiles(ColumnType, "iris")
def visit_column_type(element: ColumnType, compiler: IRISDDLCompiler, **kw) -> str:
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "%s" % format_type(compiler, element.type_),
    )


@compiles(ColumnName, "iris")
def visit_rename_column(element: ColumnName, compiler: IRISDDLCompiler, **kw) -> str:
    return "%s %s RENAME %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        format_column_name(compiler, element.newname),
    )
