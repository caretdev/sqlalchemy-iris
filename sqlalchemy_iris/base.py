import re
import intersystems_iris.dbapi._DBAPI as dbapi
from . import information_schema as ischema
from . import types
from sqlalchemy import exc
from sqlalchemy.orm import aliased
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy.sql import compiler
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import between
from sqlalchemy.sql import func
from sqlalchemy.sql.functions import ReturnTypeFromArgs
from sqlalchemy.sql import expression
from sqlalchemy.sql import schema
from sqlalchemy import sql, text
from sqlalchemy import util
from sqlalchemy import types as sqltypes

from sqlalchemy.types import BIGINT
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import INTEGER
from sqlalchemy.types import BOOLEAN
from sqlalchemy.types import DATE
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.types import TIME
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import FLOAT
from sqlalchemy.types import BINARY
from sqlalchemy.types import VARBINARY
from sqlalchemy.types import TEXT
from sqlalchemy.types import SMALLINT

ischema_names = {
    "BIGINT": BIGINT,
    "VARCHAR": VARCHAR,
    "INTEGER": INTEGER,
    "BIT": BOOLEAN,
    "DATE": DATE,
    "TIMESTAMP": TIMESTAMP,
    "NUMERIC": NUMERIC,
    "DOUBLE": FLOAT,
    "VARBINARY": BINARY,
    "LONGVARCHAR": TEXT,
    "LONGVARBINARY": VARBINARY,
    "TIME": TIME,
    "SMALLINT": SMALLINT,
    "TINYINT": SMALLINT,
}

RESERVED_WORDS = set(
    [
        "%afterhaving",
        "%allindex",
        "%alphaup",
        "%alter",
        "%begtrans",
        "%checkpriv",
        "%classname",
        "%classparameter",
        "%dbugfull",
        "%deldata",
        "%description",
        "%exact",
        "%external",
        "%file",
        "%firsttable",
        "%flatten",
        "%foreach",
        "%full",
        "%id",
        "%idadded",
        "%ignoreindex",
        "%ignoreindices",
        "%inlist",
        "%inorder",
        "%internal",
        "%intext",
        "%intrans",
        "%intransaction",
        "%key",
        "%matches",
        "%mcode",
        "%merge",
        "%minus",
        "%mvr",
        "%nocheck",
        "%nodeldata",
        "%noflatten",
        "%nofplan",
        "%noindex",
        "%nolock",
        "%nomerge",
        "%noparallel",
        "%noreduce",
        "%noruntime",
        "%nosvso",
        "%notopopt",
        "%notrigger",
        "%nounionoropt",
        "%numrows",
        "%odbcin",
        "%odbcout",
        "%parallel",
        "%plus",
        "%profile",
        "%profile_all",
        "%publicrowid",
        "%routine",
        "%rowcount",
        "%runtimein",
        "%runtimeout",
        "%startswith",
        "%starttable",
        "%sqlstring",
        "%sqlupper",
        "%string",
        "%tablename",
        "%truncate",
        "%upper",
        "%value",
        "%vid",
        "absolute",
        "add",
        "all",
        "allocate",
        "alter",
        "and",
        "any",
        "are",
        "as",
        "asc",
        "assertion",
        "at",
        "authorization",
        "avg",
        "begin",
        "between",
        "bit",
        "bit_length",
        "both",
        "by",
        "cascade",
        "case",
        "cast |",
        "char",
        "character",
        "character_length",
        "char_length",
        "check",
        "close",
        "coalesce",
        "collate",
        "commit",
        "connect",
        "connection",
        "constraint",
        "constraints",
        "continue",
        "convert",
        "corresponding",
        "count",
        "create",
        "cross",
        "current",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "cursor",
        "date",
        "deallocate",
        "dec",
        "decimal",
        "declare",
        "default",
        "deferrable",
        "deferred",
        "delete",
        "desc",
        "describe",
        "descriptor",
        "diagnostics",
        "disconnect",
        "distinct",
        "domain",
        "double",
        "drop",
        "else",
        "end",
        "endexec",
        "escape",
        "except",
        "exception",
        "exec",
        "execute",
        "exists",
        "external",
        "extract",
        "false",
        "fetch",
        "first",
        "float",
        "for",
        "foreign",
        "found",
        "from",
        "full",
        "get",
        "global",
        "go",
        "goto",
        "grant",
        "group",
        "having",
        "hour",
        "identity",
        "immediate",
        "in",
        "indicator",
        "initially",
        "inner",
        "input",
        "insensitive",
        "insert",
        "int",
        "integer",
        "intersect",
        "interval",
        "into",
        "is",
        "isolation",
        "join",
        "language",
        "last",
        "leading",
        "left",
        "level",
        "like",
        "local",
        "lower",
        "match",
        "max",
        "min",
        "minute",
        "module",
        "names",
        "national",
        "natural",
        "nchar",
        "next",
        "no",
        "not",
        "null",
        "nullif",
        "numeric",
        "octet_length",
        "of",
        "on",
        "only",
        "open",
        "option",
        "or",
        "outer",
        "output",
        "overlaps",
        "pad",
        "partial",
        "prepare",
        "preserve",
        "primary",
        "prior",
        "privileges",
        "procedure",
        "public",
        "read",
        "real",
        "references",
        "relative",
        "restrict",
        "revoke",
        "right",
        "role",
        "rollback",
        "rows",
        "schema",
        "scroll",
        "second",
        "section",
        "select",
        "session_user",
        "set",
        "shard",
        "smallint",
        "some",
        "space",
        "sqlerror",
        "sqlstate",
        "statistics",
        "substring",
        "sum",
        "sysdate",
        "system_user",
        "table",
        "temporary",
        "then",
        "time",
        "timezone_hour",
        "timezone_minute",
        "to",
        "top",
        "trailing",
        "transaction",
        "trim",
        "true",
        "union",
        "unique",
        "update",
        "upper",
        "user",
        "using",
        "values",
        "varchar",
        "varying",
        "when",
        "whenever",
        "where",
        "with",
        "work",
        "write",
    ]
)


class IRISCompiler(sql.compiler.SQLCompiler):
    """IRIS specific idiosyncrasies"""

    def limit_clause(self, select, **kw):
        return ""

    def fetch_clause(self, select, **kw):
        return ""

    def visit_empty_set_expr(self, type_):
        return "SELECT 1 WHERE 1!=1"

    def _get_limit_or_fetch(self, select):
        if select._fetch_clause is None:
            return select._limit_clause
        else:
            return select._fetch_clause

    def visit_delete(self, delete_stmt, **kw):
        if not delete_stmt._where_criteria and delete_stmt.table.foreign_keys:
            # https://community.intersystems.com/post/sql-foreign-key-constraint-check-delete
            table = delete_stmt.table
            nocheck = False
            for fk in table.foreign_keys:
                nocheck = not fk.ondelete and fk.parent.table == table
                if not nocheck:
                    break

            if nocheck is True:
                delete_stmt = delete_stmt.prefix_with('%NOCHECK', dialect='iris')
        text = super().visit_delete(delete_stmt, **kw)
        return text

    def for_update_clause(self, select, **kw):
        return ""

    def visit_true(self, expr, **kw):
        return "1"

    def visit_false(self, expr, **kw):
        return "0"

    def visit_is_true_unary_operator(self, element, operator, **kw):
        return "%s = 1" % self.process(element.element, **kw)

    def visit_is_false_unary_operator(self, element, operator, **kw):
        return "%s = 0" % self.process(element.element, **kw)

    def get_select_precolumns(self, select, **kw):

        text = ""
        if select._distinct or select._distinct_on:
            if select._distinct_on:
                text += (
                    "DISTINCT ON ("
                    + ", ".join(
                        [
                            self.process(col, **kw)
                            for col in select._distinct_on
                        ]
                    )
                    + ") "
                )
            else:
                text += "DISTINCT "

        if select._has_row_limiting_clause and self._use_top(select):
            text += "TOP %s " % self.process(
                self._get_limit_or_fetch(select), **kw
            )

        return text

    def _use_top(self, select):
        return (select._offset_clause is None) and (
            select._simple_int_clause(select._limit_clause)
            or select._simple_int_clause(select._fetch_clause)
        )

    def visit_irisexact_func(self, fn, **kw):
        return "%EXACT" + self.function_argspec(fn)

    def _use_exact_for_ordered_string(self, select):
        """
        `SELECT string_value FROM some_table ORDER BY string_value`
        Will return `string_value` in uppercase
        So, this method fixes query to use %EXACT() function
        `SELECT %EXACT(string_value) AS string_value FROM some_table ORDER BY string_value`
        """
        def _add_exact(column):
            if isinstance(column.type, sqltypes.String):
                return IRISExact(column).label(column._label if column._label else column.name)
            return column

        _order_by_clauses = [
            sql_util.unwrap_label_reference(elem)
            for elem in select._order_by_clause.clauses if isinstance(elem, schema.Column)
        ]
        if _order_by_clauses:
            select._raw_columns = [
                (_add_exact(c) if isinstance(c, schema.Column) and c in _order_by_clauses else c)
                for c in select._raw_columns
            ]

        return select

    def translate_select_structure(self, select_stmt, **kwargs):
        select = select_stmt
        if getattr(select, "_iris_visit", None) is True:
            return select

        select._iris_visit = True
        select = select._generate()

        select = self._use_exact_for_ordered_string(select)

        if not (
            select._has_row_limiting_clause
            and not self._use_top(select)
        ):
            return select

        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.

        """
        _order_by_clauses = [
            sql_util.unwrap_label_reference(elem)
            for elem in select._order_by_clause.clauses
        ]
        if not _order_by_clauses:
            _order_by_clauses = [text('%id')]

        limit_clause = self._get_limit_or_fetch(select)
        offset_clause = select._offset_clause

        label = "iris_rn"
        select = (
            select.add_columns(
                sql.func.ROW_NUMBER()
                .over(order_by=_order_by_clauses)
                .label(label)
            )
            .order_by(None)
            .alias()
        )

        iris_rn = sql.column(label)
        limitselect = sql.select(
            *[c for c in select.c if c.key != label]
        )
        if offset_clause is not None:
            if limit_clause is not None:
                limitselect = limitselect.where(
                    between(iris_rn, offset_clause + 1,
                            limit_clause + offset_clause)
                )
            else:
                limitselect = limitselect.where(iris_rn > offset_clause)
        else:
            limitselect = limitselect.where(iris_rn <= (limit_clause))
        return limitselect

    def order_by_clause(self, select, **kw):
        order_by = self.process(select._order_by_clause, **kw)

        if order_by and (not self.is_subquery() or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""

    def visit_column(self, column, within_columns_clause=False, **kwargs):
        text = super().visit_column(column, within_columns_clause=within_columns_clause, **kwargs)
        if within_columns_clause:
            return text
        if isinstance(column.type, sqltypes.Text):
            text = 'CONVERT(VARCHAR, %s)' % (text, )
        return text

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "STRING(%s, %s)" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_mod_binary(self, binary, operator, **kw):
        return (
            self.process(binary.left, **kw)
            + " # "
            + self.process(binary.right, **kw)
        )


class IRISDDLCompiler(sql.compiler.DDLCompiler):
    """IRIS syntactic idiosyncrasies"""

    def visit_create_schema(self, create, **kw):
        return ""

    def visit_drop_schema(self, drop, **kw):
        return ""

    def visit_check_constraint(self, constraint, **kw):
        pass

    def visit_add_constraint(self, create, **kw):
        if isinstance(create.element, schema.CheckConstraint):
            raise exc.CompileError("Can't add CHECK constraint")
        return super().visit_add_constraint(create, **kw)

    def visit_computed_column(self, generated, **kwargs):
        text = self.sql_compiler.process(
            generated.sqltext, include_table=True, literal_binds=True
        )
        text = re.sub(r"(?<!')(\b[^\d]\w+\b)", r'{\g<1>}', text)
        # text = text.replace("'", '"')
        text = 'COMPUTECODE {Set {*} = %s}' % (text,)
        if generated.persisted is False:
            text += ' CALCULATED'
        else:
            text += ' COMPUTEONCHANGE ("%%UPDATE")'
        return text

    def get_column_specification(self, column, **kwargs):

        colspec = [
            self.preparer.format_column(column),
        ]

        if (
            column.primary_key
            and column is column.table._autoincrement_column
        ):
            # colspec.append("SERIAL")
            # IDENTITY and ALLOWIDENTITYINSERT = 1 in table instead of SERIAL to solve issue with LAST_IDENTITY()
            colspec.append("IDENTITY")
        else:
            colspec.append(
                self.dialect.type_compiler.process(
                    column.type,
                    type_expression=column,
                    identifier_preparer=self.preparer,
                )
            )

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append("DEFAULT " + default)

        if column.computed is not None:
            colspec.append(self.process(column.computed))

        if not column.nullable:
            colspec.append("NOT NULL")

        comment = column.comment
        if comment is not None:
            literal = self.sql_compiler.render_literal_value(
                comment, sqltypes.String()
            )
            colspec.append("%DESCRIPTION " + literal)

        return " ".join(colspec)

    def post_create_table(self, table):
        return " WITH %CLASSPARAMETER ALLOWIDENTITYINSERT = 1"

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True, **kw
    ):
        text = super().visit_create_index(create, include_schema, include_table_schema, **kw)

        index = create.element
        preparer = self.preparer

        # handle other included columns
        includeclause = index.dialect_options["iris"]["include"]
        if includeclause:
            inclusions = [
                index.table.c[col]
                if isinstance(col, util.string_types)
                else col
                for col in includeclause
            ]

            text += " WITH DATA (%s)" % ", ".join(
                [preparer.quote(c.name) for c in inclusions]
            )

        return text


class IRISTypeCompiler(compiler.GenericTypeCompiler):
    def visit_boolean(self, type_, **kw):
        return "BIT"


class IRISIdentifierPreparer(sql.compiler.IdentifierPreparer):
    """Install IRIS specific reserved words."""

    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(RESERVED_WORDS)
    illegal_initial_characters = compiler.ILLEGAL_INITIAL_CHARACTERS.union(
        ["_"]
    )

    def __init__(self, dialect):
        super(IRISIdentifierPreparer, self).__init__(
            dialect, omit_schema=False)


class IRISExecutionContext(default.DefaultExecutionContext):

    def get_lastrowid(self):
        try:
            return self.cursor.lastrowid
        except Exception:
            cursor = self.cursor
            cursor.execute("SELECT LAST_IDENTITY()")
            lastrowid = cursor.fetchone()[0]
            return lastrowid

    def create_cursor(self):
        cursor = self._dbapi_connection.cursor()
        return cursor


colspecs = {
    sqltypes.Boolean: types.IRISBoolean,
    sqltypes.Date: types.IRISDate,
    sqltypes.DateTime: types.IRISDateTime,
    sqltypes.TIMESTAMP: types.IRISTimeStamp,
    sqltypes.Time: types.IRISTime,
}


class IRISExact(ReturnTypeFromArgs):
    """The IRIS SQL %EXACT() function."""

    inherit_cache = True


class IRISDialect(default.DefaultDialect):

    name = 'iris'

    default_schema_name = "SQLUser"

    default_paramstyle = "format"
    supports_statement_cache = True

    supports_native_decimal = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_alter = True
    supports_schemas = True
    supports_views = True
    supports_default_values = True

    supports_native_boolean = True
    non_native_boolean_check_constraint = False

    supports_sequences = False

    postfetch_lastrowid = True
    supports_simple_order_by_label = False
    supports_empty_insert = False
    supports_is_distinct_from = False

    colspecs = colspecs

    ischema_names = ischema_names

    statement_compiler = IRISCompiler
    ddl_compiler = IRISDDLCompiler
    preparer = IRISIdentifierPreparer
    type_compiler = IRISTypeCompiler
    execution_ctx_cls = IRISExecutionContext

    construct_arguments = [
        (schema.Index, {"include": None}),
    ]

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    _isolation_lookup = set(
        [
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "READ VERIFIED",
        ]
    )

    def _get_default_schema_name(self, connection):
        return IRISDialect.default_schema_name

    def _get_option(self, connection, option):
        cursor = connection.cursor()
        # cursor = connection.cursor()
        cursor.execute('SELECT %SYSTEM_SQL.Util_GetOption(?)', option)
        row = cursor.fetchone()
        if row:
            return row[0]
        return None

    def _set_option(self, connection, option, value):
        cursor = connection.cursor()
        # cursor = connection.cursor()
        cursor.execute('SELECT %SYSTEM_SQL.Util_SetOption(?, ?)', option, value)
        row = cursor.fetchone()
        if row:
            return row[0]
        return None

    def get_isolation_level(self, connection):
        level = int(self._get_option(connection, 'IsolationMode'))
        if level == 0:
            return 'READ UNCOMMITTED'
        elif level == 1:
            return 'READ COMMITTED'
        elif level == 3:
            return 'READ VERIFIED'
        return None

    def set_isolation_level(self, connection, level_str):
        if level_str == "AUTOCOMMIT":
            connection.setAutoCommit(True)
        else:
            connection.setAutoCommit(False)
            level = 0
            if level_str == 'READ COMMITTED':
                level = 1
            elif level_str == 'READ VERIFIED':
                level = 3
            self._set_option(connection, 'IsolationMode', level)

    @classmethod
    def dbapi(cls):
        # dbapi.paramstyle = "format"
        return dbapi

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.InterfaceError):
            return "Connection is closed" in str(e)
        return False

    def do_ping(self, dbapi_connection):
        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(self._dialect_specific_select_one)
            finally:
                cursor.close()
        except self.dbapi.Error as err:
            if self.is_disconnect(err, dbapi_connection, cursor):
                return False
            else:
                raise
        else:
            return True

    def create_connect_args(self, url):
        opts = {}
        opts["hostname"] = url.host
        opts["port"] = int(url.port) if url.port else 1972
        opts["namespace"] = url.database if url.database else 'USER'
        opts["username"] = url.username if url.username else ''
        opts["password"] = url.password if url.password else ''

        opts['autoCommit'] = False

        return ([], opts)

    def do_execute(self, cursor, query, params, context=None):
        cursor.execute(query, *params)

    def do_executemany(self, cursor, query, params, context=None):
        cursor.executemany(query, params)

    def do_begin(self, connection):
        pass
        # connection.cursor().execute("START TRANSACTION")

    def do_rollback(self, connection):
        connection.rollback()

    def do_commit(self, connection):
        connection.commit()

    def do_savepoint(self, connection, name):
        connection.execute(expression.SavepointClause(name))

    def do_release_savepoint(self, connection, name):
        pass

    def get_schema(self, schema=None):
        if schema is None:
            return 'SQLUser'
        return schema

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = sql.select(ischema.schemata.c.schema_name).order_by(
            ischema.schemata.c.schema_name
        )
        schema_names = [r[0] for r in connection.execute(s)]
        return schema_names

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        tables = ischema.tables
        schema_name = self.get_schema(schema)
        s = (
            sql.select(tables.c.table_name)
            .where(
                sql.and_(
                    tables.c.table_schema == str(schema_name),
                    tables.c.table_type == "BASE TABLE",
                )
            )
            .order_by(tables.c.table_name)
        )
        table_names = [r[0] for r in connection.execute(s)]
        return table_names

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        self._ensure_has_table_connection(connection)
        tables = ischema.tables
        schema_name = self.get_schema(schema)

        s = (
            sql.select(func.count())
            .where(
                sql.and_(
                    tables.c.table_schema == str(schema_name),
                    tables.c.table_name == str(table_name),
                )
            )
        )
        return bool(connection.execute(s).scalar())

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, unique=False, **kw):
        schema_name = self.get_schema(schema)
        indexes = ischema.indexes
        tables = ischema.tables
        index_def = ischema.index_definition

        s = (
            sql.select(
                indexes.c.index_name,
                indexes.c.column_name,
                indexes.c.primary_key,
                indexes.c.non_unique,
                indexes.c.asc_or_desc,
                index_def.c.Data,
            )
            .select_from(indexes)
            .outerjoin(
                index_def,
                sql.and_(
                    index_def.c.SqlName == indexes.c.index_name,
                    index_def.c.parent ==
                    sql.select(tables.c.classname)
                    .where(
                        indexes.c.table_name == tables.c.table_name,
                        indexes.c.table_schema == tables.c.table_schema,
                    ).scalar_subquery()
                ),
            )
            .where(
                sql.and_(
                    indexes.c.table_schema == str(schema_name),
                    indexes.c.table_name == str(table_name),
                    indexes.c.primary_key == sql.false(),
                    (indexes.c.non_unique == sql.true()) if not unique else (1 == 1)
                )
            )
            .order_by(indexes.c.ordinal_position)
        )

        rs = connection.execute(s)

        indexes = util.defaultdict(dict)
        for row in rs:
            (
                idxname,
                colname,
                _,
                nuniq,
                _,
                include,
            ) = row

            indexrec = indexes[idxname]
            if "name" not in indexrec:
                indexrec["name"] = self.normalize_name(idxname)
                indexrec["column_names"] = []
                indexrec["unique"] = not nuniq

            indexrec["column_names"].append(
                self.normalize_name(colname)
            )
            include = include.split(',') if include else []
            indexrec["include_columns"] = include
            if include:
                indexrec["dialect_options"] = {
                    "iris_include": include
                }

        indexes = list(indexes.values())
        return indexes

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        schema_name = self.get_schema(schema)
        key_constraints = ischema.key_constraints
        constraints = ischema.constraints

        s = (
            sql.select(
                key_constraints.c.constraint_name,
                key_constraints.c.column_name,
            )
            .join(constraints,
                  sql.and_(
                      key_constraints.c.constraint_name == constraints.c.constraint_name,
                      key_constraints.c.table_schema == constraints.c.table_schema,
                  )
                  )
            .where(
                sql.and_(
                    key_constraints.c.table_schema == str(schema_name),
                    key_constraints.c.table_name == str(table_name),
                    constraints.c.constraint_type == "PRIMARY KEY",
                )
            )
            .order_by(key_constraints.c.ordinal_position)
        )

        rs = connection.execute(s)

        constraint_name = None
        pkfields = []
        for row in rs:
            (
                name,
                colname,
            ) = row
            constraint_name = self.normalize_name(name)
            pkfields.append(self.normalize_name(colname))

        if pkfields:
            return {
                "constrained_columns": pkfields,
                "name": constraint_name,
            }

        return None

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        indexes = self.get_indexes(
            connection, table_name, schema, unique=True, **kw)
        return [{'name': i['name'], 'column_names': i['column_names']}
                for i in indexes if i['unique']]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        schema_name = self.get_schema(schema)
        ref_constraints = ischema.ref_constraints
        key_constraints = ischema.key_constraints
        key_constraints_ref = aliased(ischema.key_constraints)

        s = (
            sql.select(
                key_constraints.c.constraint_name,
                key_constraints.c.column_name,
                key_constraints_ref.c.table_schema,
                key_constraints_ref.c.table_name,
                key_constraints_ref.c.column_name,
                ref_constraints.c.match_option,
                ref_constraints.c.update_rule,
                ref_constraints.c.delete_rule,
            )
            .join(
                key_constraints,
                sql.and_(
                    key_constraints.c.table_schema == ref_constraints.c.constraint_schema,
                    key_constraints.c.constraint_name == ref_constraints.c.constraint_name,
                )
            )
            .join(
                key_constraints_ref,
                sql.and_(
                    key_constraints_ref.c.constraint_schema == ref_constraints.c.unique_constraint_schema,
                    key_constraints_ref.c.constraint_name == ref_constraints.c.unique_constraint_name,
                    key_constraints_ref.c.ordinal_position == key_constraints.c.ordinal_position,
                )
            )
            .where(
                sql.and_(
                    key_constraints.c.table_schema == str(schema_name),
                    key_constraints.c.table_name == str(table_name),
                )
            )
            .order_by(key_constraints_ref.c.ordinal_position)
        )

        rs = connection.execute(s)

        fkeys = []

        def fkey_rec():
            return {
                "name": None,
                "constrained_columns": [],
                "referred_schema": None,
                "referred_table": None,
                "referred_columns": [],
                "options": {},
            }

        fkeys = util.defaultdict(fkey_rec)

        for row in rs:
            (
                rfknm,
                scol,
                rschema,
                rtbl,
                rcol,
                _,  # match rule
                fkuprule,
                fkdelrule,
            ) = row

            rec = fkeys[rfknm]
            rec["name"] = rfknm

            if fkuprule != "NO ACTION":
                rec["options"]["onupdate"] = fkuprule

            if fkdelrule != "NO ACTION":
                rec["options"]["ondelete"] = fkdelrule

            if not rec["referred_table"]:
                rec["referred_table"] = rtbl
                if rschema != 'SQLUser':
                    rec["referred_schema"] = rschema

            local_cols, remote_cols = (
                rec["constrained_columns"],
                rec["referred_columns"],
            )

            local_cols.append(scol)
            remote_cols.append(rcol)

        if fkeys:
            return list(fkeys.values())

        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        schema_name = self.get_schema(schema)
        tables = ischema.tables
        columns = ischema.columns
        property = ischema.property_definition

        whereclause = sql.and_(
            columns.c.table_name == str(table_name),
            columns.c.table_schema == str(schema_name),
        )

        s = (
            sql.select(
                columns.c.column_name,
                columns.c.data_type,
                columns.c.is_nullable,
                columns.c.character_maximum_length,
                columns.c.numeric_precision,
                columns.c.numeric_scale,
                columns.c.column_default,
                columns.c.collation_name,
                columns.c.auto_increment,
                property.c.SqlComputeCode,
                property.c.Calculated,
                property.c.Transient,
                # columns.c.description,
            )
            .select_from(columns)
            .outerjoin(
                property,
                sql.and_(
                    property.c.SqlFieldName == columns.c.column_name,
                    property.c.parent ==
                    sql.select(tables.c.classname)
                    .where(
                        columns.c.table_name == tables.c.table_name,
                        columns.c.table_schema == tables.c.table_schema,
                    ).scalar_subquery()
                ),
            )
            .where(whereclause)
            .order_by(columns.c.ordinal_position)
        )

        c = connection.execution_options(future_result=True).execute(s)

        cols = []
        for row in c.mappings():
            name = row[columns.c.column_name]
            type_ = row[columns.c.data_type].upper()
            nullable = row[columns.c.is_nullable]
            charlen = row[columns.c.character_maximum_length]
            numericprec = row[columns.c.numeric_precision]
            numericscale = row[columns.c.numeric_scale]
            default = row[columns.c.column_default]
            collation = row[columns.c.collation_name]
            autoincrement = row[columns.c.auto_increment]
            sqlComputeCode = row[property.c.SqlComputeCode]
            calculated = row[property.c.Calculated]
            transient = row[property.c.Transient]
            # description = row[columns.c.description]

            coltype = self.ischema_names.get(type_, None)

            kwargs = {}
            if coltype in (
                VARCHAR,
                BINARY,
                TEXT,
                VARBINARY,
            ):
                if charlen == -1:
                    charlen = None
                kwargs["length"] = int(charlen)
                if collation:
                    kwargs["collation"] = collation
            if coltype is None:
                util.warn(
                    "Did not recognize type '%s' of column '%s'"
                    % (type_, name)
                )
                coltype = sqltypes.NULLTYPE
            else:
                if issubclass(coltype, sqltypes.Numeric):
                    kwargs["precision"] = int(numericprec)

                    if not issubclass(coltype, sqltypes.Float):
                        kwargs["scale"] = int(numericscale)

                coltype = coltype(**kwargs)

            cdict = {
                "name": name,
                "type": coltype,
                "nullable": nullable,
                "default": default,
                "autoincrement": autoincrement,
                # "comment": description,
            }
            if sqlComputeCode and 'set {*} = ' in sqlComputeCode.lower():
                sqltext = sqlComputeCode
                sqltext = sqltext.split(' = ')[1]
                sqltext = re.sub(r"{(\b\w+\b)}", r"\g<1>", sqltext)
                persisted = not calculated and not transient
                cdict['computed'] = {
                    "sqltext": sqltext,
                    "persisted": persisted,
                }
            cols.append(cdict)

        return cols

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        schema_name = self.get_schema(schema)
        views = ischema.views
        s = (
            sql.select(views.c.table_name)
            .where(
                views.c.table_schema == str(schema_name),
            )
            .order_by(views.c.table_name)
        )
        view_names = [r[0] for r in connection.execute(s)]
        return view_names

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        schema_name = self.get_schema(schema)
        views = ischema.views

        view_def = connection.execute(
            sql.select(views.c.view_definition)
            .where(
                views.c.table_schema == str(schema_name),
                views.c.table_name == str(view_name),
            )
        ).scalar()

        if view_def:
            return view_def
        return None
