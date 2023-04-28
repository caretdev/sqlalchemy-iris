import logging

from sqlalchemy.ext.compiler import compiles
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

    def correct_for_autogen_constraints(
        self,
        conn_unique_constraints,
        conn_indexes,
        metadata_unique_constraints,
        metadata_indexes,
    ):

        doubled_constraints = {
            index
            for index in conn_indexes
            if index.info.get("duplicates_constraint")
        }

        for ix in doubled_constraints:
            conn_indexes.remove(ix)

        # if not sqla_compat.sqla_2:
        #     self._skip_functional_indexes(metadata_indexes, conn_indexes)

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
