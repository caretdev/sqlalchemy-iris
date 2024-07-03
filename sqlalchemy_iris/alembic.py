import logging
import re

from typing import Optional
from typing import Any

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.schema import CheckConstraint
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.sql import table
from sqlalchemy import types

from alembic.ddl import DefaultImpl
from alembic.ddl.base import ColumnNullable
from alembic.ddl.base import ColumnType
from alembic.ddl.base import ColumnName
from alembic.ddl.base import AddColumn
from alembic.ddl.base import DropColumn
from alembic.ddl.base import Column
from alembic.ddl.base import alter_table
from alembic.ddl.base import alter_column
from alembic.ddl.base import drop_column
from alembic.ddl.base import format_type
from alembic.ddl.base import format_column_name
from .base import IRISDDLCompiler

log = logging.getLogger(__name__)

# IRIS Interprets these types as %Streams, and no direct type change is available
_as_stream = [
    types.LargeBinary,
    types.BLOB,
    types.CLOB,
]


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

        if rendered_metadata_default is not None:
            rendered_metadata_default = re.sub(
                r"[\(\) \"\']", "", rendered_metadata_default
            )

        if rendered_inspector_default is not None:
            rendered_inspector_default = re.sub(
                r"[\(\) \"\']", "", rendered_inspector_default
            )

        return rendered_inspector_default != rendered_metadata_default

    def alter_column(
        self,
        table_name: str,
        column_name: str,
        type_: Optional[TypeEngine] = None,
        existing_type: Optional[TypeEngine] = None,
        schema: Optional[str] = None,
        name: Optional[str] = None,
        **kw: Any,
    ) -> None:
        if existing_type.__class__ not in _as_stream and type_.__class__ in _as_stream:
            """
            To change column type to %Stream
            * rename the column with a new name with suffix `__superset_tmp`
            * create a new column with the old name
            * copy data from an old column to new column
            * drop old column
            * fix missing parameters, such as nullable
            """
            tmp_column = f"{column_name}__superset_tmp"
            self._exec(ColumnName(table_name, column_name, tmp_column, schema=schema))
            new_kw = {}
            self._exec(
                AddColumn(
                    table_name,
                    Column(column_name, type_=type_, **new_kw),
                    schema=schema,
                )
            )
            tab = table(
                table_name,
                Column(column_name, key="new_col"),
                Column(tmp_column, key="old_col"),
                schema=schema,
            )
            self._exec(tab.update().values({tab.c.new_col: tab.c.old_col}))
            self._exec(DropColumn(table_name, Column(tmp_column), schema=schema))
            new_kw = {}
            for k in ["server_default", "nullable", "autoincrement"]:
                if f"existing_{k}" in kw:
                    new_kw[k] = kw[f"existing_{k}"]
            return super().alter_column(
                table_name, column_name, schema=schema, name=name, **new_kw
            )
        return super().alter_column(
            table_name,
            column_name,
            type_=type_,
            existing_type=existing_type,
            schema=schema,
            name=name,
            **kw,
        )

    def add_constraint(self, const: Any) -> None:
        if isinstance(const, CheckConstraint):
            # just ignore it
            return
        super().add_constraint(const)


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


@compiles(DropColumn, "iris")
def visit_drop_column(element: DropColumn, compiler: IRISDDLCompiler, **kw) -> str:
    return "%s %s CASCADE" % (
        alter_table(compiler, element.table_name, element.schema),
        drop_column(compiler, element.column.name, **kw),
    )
