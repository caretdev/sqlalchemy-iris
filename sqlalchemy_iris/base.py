import re
import intersystems_iris.dbapi._DBAPI as dbapi
import intersystems_iris._IRISNative as IRISNative
from . import information_schema as ischema
from sqlalchemy import exc
from sqlalchemy.orm import aliased
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy.sql import compiler
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import between
from sqlalchemy.sql import func
from sqlalchemy.sql.functions import ReturnTypeFromArgs
from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql import expression
from sqlalchemy.sql import schema
from sqlalchemy import sql, text
from sqlalchemy import util
from sqlalchemy import types as sqltypes

from sqlalchemy import __version__ as sqlalchemy_version

if sqlalchemy_version.startswith("2."):
    from sqlalchemy.engine import ObjectKind
    from sqlalchemy.engine import ObjectScope
    from sqlalchemy.engine.reflection import ReflectionDefaults
else:
    from enum import Flag

    class ObjectKind(Flag):
        TABLE = 1
        VIEW = 2
        ANY = TABLE | VIEW

    class ObjectScope(Flag):
        DEFAULT = 1
        TEMPORARY = 2
        ANY = DEFAULT | TEMPORARY

    class ReflectionDefaults:
        @classmethod
        def columns(cls):
            return []

        @classmethod
        def pk_constraint(cls):
            return {
                "name": None,
                "constrained_columns": [],
            }

        @classmethod
        def foreign_keys(cls):
            return []

        @classmethod
        def indexes(cls):
            return []

        @classmethod
        def unique_constraints(cls):
            return []

        @classmethod
        def check_constraints(cls):
            return []


from sqlalchemy.types import BIGINT
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import CHAR
from sqlalchemy.types import INTEGER
from sqlalchemy.types import DATE
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.types import TIME
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import BINARY
from sqlalchemy.types import VARBINARY
from sqlalchemy.types import TEXT
from sqlalchemy.types import SMALLINT

from .types import BIT
from .types import DOUBLE
from .types import LONGVARCHAR
from .types import LONGVARBINARY
from .types import TINYINT

from .types import IRISBoolean
from .types import IRISTime
from .types import IRISTimeStamp
from .types import IRISDate
from .types import IRISDateTime
from .types import IRISListBuild  # noqa
from .types import IRISVector  # noqa


ischema_names = {
    "BIGINT": BIGINT,
    "BIT": BIT,
    "DATE": DATE,
    "DOUBLE": DOUBLE,
    "INTEGER": INTEGER,
    "LONGVARBINARY": LONGVARBINARY,
    "LONGVARCHAR": LONGVARCHAR,
    "NUMERIC": NUMERIC,
    "SMALLINT": SMALLINT,
    "TIME": IRISTime,
    "TIMESTAMP": IRISTimeStamp,
    "TINYINT": TINYINT,
    "VARBINARY": VARBINARY,
    "VARCHAR": VARCHAR,
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

    def visit_exists_unary_operator(
        self, element, operator, within_columns_clause=False, **kw
    ):
        if within_columns_clause:
            return "(SELECT 1 WHERE EXISTS(%s))" % self.process(element.element, **kw)
        else:
            return "EXISTS(%s)" % self.process(element.element, **kw)

    def limit_clause(self, select, **kw):
        return ""

    def fetch_clause(self, select, **kw):
        return ""

    def visit_empty_set_expr(self, type_, **kw):
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
                delete_stmt = delete_stmt.prefix_with("%NOCHECK", dialect="iris")
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

    def visit_is__binary(self, binary, operator, **kw):
        op = "IS" if isinstance(binary.right, Null) else "="
        return "%s %s %s" % (
            self.process(binary.left),
            op,
            self.process(binary.right),
        )

    def visit_is_not_binary(self, binary, operator, **kw):
        op = "IS NOT" if isinstance(binary.right, Null) else "<>"
        return "%s %s %s" % (
            self.process(binary.left),
            op,
            self.process(binary.right),
        )

    def get_select_precolumns(self, select, **kw):
        text = ""
        if select._distinct or select._distinct_on:
            if select._distinct_on:
                text += (
                    "DISTINCT ON ("
                    + ", ".join(
                        [self.process(col, **kw) for col in select._distinct_on]
                    )
                    + ") "
                )
            else:
                text += "DISTINCT "

        if select._has_row_limiting_clause and self._use_top(select):
            text += "TOP %s " % self.process(self._get_limit_or_fetch(select), **kw)

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
                return IRISExact(column).label(
                    column._label if column._label else column.name
                )
            return column

        _order_by_clauses = [
            sql_util.unwrap_label_reference(elem)
            for elem in select._order_by_clause.clauses
            if isinstance(elem, schema.Column)
        ]
        if _order_by_clauses:
            select._raw_columns = [
                (
                    _add_exact(c)
                    if isinstance(c, schema.Column) and c in _order_by_clauses
                    else c
                )
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

        if not (select._has_row_limiting_clause and not self._use_top(select)):
            return select

        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.

        """
        _order_by_clauses = [
            sql_util.unwrap_label_reference(elem)
            for elem in select._order_by_clause.clauses
        ]
        if not _order_by_clauses:
            _order_by_clauses = [text("%id")]

        limit_clause = self._get_limit_or_fetch(select)
        offset_clause = select._offset_clause

        label = "iris_rn"
        select = (
            select.add_columns(
                sql.func.ROW_NUMBER().over(order_by=_order_by_clauses).label(label)
            )
            .order_by(None)
            .alias()
        )

        iris_rn = sql.column(label)
        limitselect = sql.select(*[c for c in select.c if c.key != label])
        if offset_clause is not None:
            if limit_clause is not None:
                limitselect = limitselect.where(
                    between(iris_rn, offset_clause + 1, limit_clause + offset_clause)
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

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "STRING(%s, %s)" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_concat_func(
        self, func, **kw
    ):
        args = [self.process(clause, **kw) for clause in func.clauses.clauses]
        return ' || '.join(args)

    def visit_mod_binary(self, binary, operator, **kw):
        return (
            self.process(binary.left, **kw) + " # " + self.process(binary.right, **kw)
        )

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        # InterSystems use own format for %MATCHES, it does not support Regular Expressions
        raise exc.CompileError("InterSystems IRIS does not support REGEXP")

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        # InterSystems use own format for %MATCHES, it does not support Regular Expressions
        raise exc.CompileError("InterSystems IRIS does not support REGEXP")

    def visit_case(self, clause, **kwargs):
        x = "CASE "
        if clause.value is not None:
            x += clause.value._compiler_dispatch(self, **kwargs) + " "
        for cond, result in clause.whens:
            x += (
                "WHEN "
                + cond._compiler_dispatch(self, **kwargs)
                + " THEN "
                # Explicit CAST required on 2023.1
                + (
                    self.visit_cast(sql.cast(result, result.type), **kwargs)
                    if isinstance(result, sql.elements.BindParameter)
                    else result._compiler_dispatch(self, **kwargs)
                )
                + " "
            )
        if clause.else_ is not None:
            x += (
                "ELSE "
                + (
                    self.visit_cast(sql.cast(clause.else_, clause.else_.type), **kwargs)
                    if isinstance(clause.else_, sql.elements.BindParameter)
                    else clause.else_._compiler_dispatch(self, **kwargs)
                )
                + " "
            )
        x += "END"
        return x


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
        text = re.sub(r"(?<!')(\b[^\W\d]+\w+\b)", r"{\g<1>}", text)
        # text = text.replace("'", '"')
        text = "COMPUTECODE {Set {*} = %s}" % (text,)
        if generated.persisted is False:
            text += " CALCULATED"
        else:
            text += ' COMPUTEONCHANGE ("%%UPDATE")'
        return text

    def get_column_specification(self, column, **kwargs):
        colspec = [
            self.preparer.format_column(column),
        ]

        if column.primary_key and column is column.table._autoincrement_column:
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
            literal = self.sql_compiler.render_literal_value(comment, sqltypes.String())
            colspec.append("%DESCRIPTION " + literal)

        return " ".join(colspec)

    def post_create_table(self, table):
        return " WITH %CLASSPARAMETER ALLOWIDENTITYINSERT = 1"

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True, **kw
    ):
        text = super().visit_create_index(
            create, include_schema, include_table_schema, **kw
        )

        index = create.element
        preparer = self.preparer

        # handle other included columns
        includeclause = index.dialect_options["iris"]["include"]
        if includeclause:
            inclusions = [
                index.table.c[col] if isinstance(col, str) else col
                for col in includeclause
            ]

            text += " WITH DATA (%s)" % ", ".join(
                [preparer.quote(c.name) for c in inclusions]
            )

        return text

    def visit_drop_index(self, drop, **kw):
        return "DROP INDEX %s ON %s" % (
            self._prepared_index_name(drop.element, include_schema=False),
            self.preparer.format_table(drop.element.table),
        )


class IRISTypeCompiler(compiler.GenericTypeCompiler):
    def visit_BOOLEAN(self, type_, **kw):
        return self.visit_BIT(type_)

    def visit_BIT(self, type_, **kw):
        return "BIT"

    def visit_VARCHAR(self, type_, **kw):
        # If length is not specified, use 50 as default in IRIS
        if type_.length is None:
            type_ = VARCHAR(50)
        return "VARCHAR(%d)" % type_.length

    def visit_TEXT(self, type_, **kw):
        return "VARCHAR(65535)"

    def visit_LONGVARBINARY(self, type_, **kw):
        return "LONGVARBINARY"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_UUID(self, type_, **kw):
        return "UNIQUEIDENTIFIER"


class IRISIdentifierPreparer(sql.compiler.IdentifierPreparer):
    """Install IRIS specific reserved words."""

    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(RESERVED_WORDS)
    illegal_initial_characters = compiler.ILLEGAL_INITIAL_CHARACTERS.union(["_"])

    def __init__(self, dialect):
        super(IRISIdentifierPreparer, self).__init__(dialect, omit_schema=False)

    # def _escape_identifier(self, value):
    #     value = value.replace(self.escape_quote, self.escape_to_quote)
    #     return value.replace(".", "_")

    def format_column(
        self,
        column,
        use_table=False,
        name=None,
        table_name=None,
        use_schema=False,
        anon_map=None,
    ):
        if name is None:
            name = column.name

        # if '.' in name:
        #     name = name.replace('.', '_')

        return super().format_column(
            column, use_table, name, table_name, use_schema, anon_map
        )


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
    sqltypes.Boolean: IRISBoolean,
    sqltypes.Date: IRISDate,
    sqltypes.DateTime: IRISDateTime,
    sqltypes.TIMESTAMP: IRISTimeStamp,
    sqltypes.Time: IRISTime,
}
if sqlalchemy_version.startswith("2."):
    from .types import IRISUniqueIdentifier

    colspecs[sqltypes.UUID] = IRISUniqueIdentifier


class IRISExact(ReturnTypeFromArgs):
    """The IRIS SQL %EXACT() function."""

    inherit_cache = True


class IRISDialect(default.DefaultDialect):
    name = "iris"

    embedded = False

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

    supports_multivalues_insert = True

    supports_sequences = False

    returns_native_bytes = True

    div_is_floordiv = False

    postfetch_lastrowid = True
    supports_simple_order_by_label = False
    supports_empty_insert = False
    supports_is_distinct_from = False

    supports_vectors = None

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

    def _get_server_version_info(self, connection):
        server_version = connection.connection._connection_info._server_version
        server_version = (
            server_version[server_version.find("Version") + 8 :]
            .split(" ")[0]
            .split(".")
        )
        return tuple([int("".join(filter(str.isdigit, v))) for v in server_version])

    _isolation_lookup = set(
        [
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "READ VERIFIED",
        ]
    )

    def _get_default_schema_name(self, connection):
        return IRISDialect.default_schema_name

    def on_connect(self):
        super_ = super().on_connect()

        def on_connect(conn):
            if super_ is not None:
                super_(conn)

            try:
                with conn.cursor() as cursor:
                    # Distance or similarity
                    cursor.execute(
                        "select vector_cosine(to_vector('1'), to_vector('1'))"
                    )
                self.supports_vectors = True
            except:  # noqa
                self.supports_vectors = False
            self._dictionary_access = False
            with conn.cursor() as cursor:
                cursor.execute("%CHECKPRIV SELECT ON %Dictionary.PropertyDefinition")
                self._dictionary_access = cursor.sqlcode == 0

            # if not self.supports_vectors:
            #     util.warn("No native support for VECTOR or not activated by license")
            if not self._dictionary_access:
                util.warn(
                    """
There are no access to %Dictionary, may be required for some advanced features,
 such as Calculated fields, and include columns in indexes
                """.replace(
                        "\n", ""
                    )
                )

        return on_connect

    def _get_option(self, connection, option):
        with connection.cursor() as cursor:
            cursor.execute("SELECT %SYSTEM_SQL.Util_GetOption(?)", option)
            row = cursor.fetchone()
            if row:
                return row[0]
        return None

    def _set_option(self, connection, option, value):
        with connection.cursor() as cursor:
            cursor.execute("SELECT %SYSTEM_SQL.Util_SetOption(?, ?)", [option, value])
            row = cursor.fetchone()
            if row:
                return row[0]
        return None

    def get_isolation_level_values(self, dbapi_connection):
        levels = set(self._isolation_lookup)
        levels.add("AUTOCOMMIT")
        return levels

    def get_isolation_level(self, connection):
        try:
            level = int(self._get_option(connection, "IsolationMode"))
        except dbapi.DatabaseError:
            # caught access violation error
            # by default it's 0
            level = 0
        if level == 0:
            return "READ UNCOMMITTED"
        elif level == 1:
            return "READ COMMITTED"
        elif level == 3:
            return "READ VERIFIED"
        return None

    def set_isolation_level(self, connection, level_str):
        if level_str == "AUTOCOMMIT":
            connection.setAutoCommit(True)
        else:
            connection.setAutoCommit(False)
            if level_str not in ["READ COMMITTED", "READ VERIFIED"]:
                level_str = "READ UNCOMMITTED"
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL " + level_str)

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

        opts["application_name"] = "sqlalchemy"
        opts["hostname"] = url.host
        opts["port"] = int(url.port) if url.port else 1972
        opts["namespace"] = url.database if url.database else "USER"
        opts["username"] = url.username if url.username else ""
        opts["password"] = url.password if url.password else ""

        opts["autoCommit"] = False

        opts["embedded"] = self.embedded
        if opts["hostname"] and "@" in opts["hostname"]:
            _h = opts["hostname"].split("@")
            opts["password"] += "@" + _h[0 : len(_h) - 1].join("@")
            opts["hostname"] = _h[len(_h) - 1]

        return ([], opts)

    _debug_queries = False
    # _debug_queries = True

    def _debug(self, query, params, many=False):
        from decimal import Decimal

        if not self._debug_queries:
            return
        if many:
            for p in params:
                self._debug(query, p)
            return
        for p in params:
            if isinstance(p, Decimal):
                v = str(p)
            elif p is None:
                v = "NULL"
            else:
                v = "%r" % (p,)
            query = query.replace("?", v, 1)
        print("--")
        print(query + ";")
        print("--")

    def _debug_pre(self, query, params, many=False):
        print("-- do_execute" + "many" if many else "")
        if not self._debug_queries:
            return
        for line in query.split("\n"):
            print("-- ", line)
        if many:
            print(params)
        else:
            for p in params:
                print("-- @param = %r" % (p,))

    def do_execute(self, cursor, query, params, context=None):
        if query.endswith(";"):
            query = query[:-1]
        self._debug(query, params)
        cursor.execute(query, params)

    def do_executemany(self, cursor, query, params, context=None):
        if query.endswith(";"):
            query = query[:-1]
        self._debug(query, params, True)
        cursor.executemany(query, params)

    def do_begin(self, connection):
        pass

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
            return "SQLUser"
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
    def get_temp_table_names(self, connection, dblink=None, **kw):
        tables = ischema.tables
        s = (
            sql.select(tables.c.table_name)
            .where(
                sql.and_(
                    tables.c.table_schema == self.default_schema_name,
                    tables.c.table_type == "GLOBAL TEMPORARY",
                )
            )
            .order_by(tables.c.table_name)
        )
        table_names = [r[0] for r in connection.execute(s)]
        return table_names

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        tables = ischema.tables
        schema_name = self.get_schema(schema)

        s = sql.select(func.count()).where(
            sql.and_(
                tables.c.table_schema == str(schema_name),
                tables.c.table_name == str(table_name),
            )
        )
        return bool(connection.execute(s).scalar())

    def _get_all_objects(self, connection, schema, filter_names, scope, kind, **kw):
        tables = ischema.tables
        schema_name = self.get_schema(schema)

        s = (
            sql.select(
                tables.c.table_name,
            )
            .select_from(tables)
            .where(
                tables.c.table_schema == str(schema_name),
            )
        )

        table_types = []
        if ObjectScope.TEMPORARY in scope and ObjectKind.TABLE in kind:
            table_types.append("GLOBAL TEMPORARY")
        if ObjectScope.DEFAULT in scope and ObjectKind.VIEW in kind:
            table_types.append("VIEW")
        if ObjectScope.DEFAULT in scope and ObjectKind.TABLE in kind:
            table_types.append("BASE TABLE")

        if not table_types:
            return []
        s = s.where(tables.c.table_type.in_(table_types))

        if filter_names:
            s = s.where(tables.c.table_name.in_([str(name) for name in filter_names]))

        result = connection.execute(s).scalars()
        return result.all()

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, unique=False, **kw):
        data = self.get_multi_indexes(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            unique=unique,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    def get_multi_indexes(
        self, connection, schema, filter_names, scope, kind, unique=False, **kw
    ):
        schema_name = self.get_schema(schema)
        indexes = ischema.indexes
        tables = ischema.tables
        index_def = ischema.index_definition

        all_objects = self._get_all_objects(
            connection, schema, filter_names, scope, kind
        )
        if not all_objects:
            return util.defaultdict(list)

        s = (
            sql.select(
                indexes.c.table_name,
                indexes.c.index_name,
                indexes.c.column_name,
                indexes.c.primary_key,
                indexes.c.non_unique,
                indexes.c.asc_or_desc,
            )
            .select_from(indexes)
            .where(
                sql.and_(
                    indexes.c.table_schema == str(schema_name),
                    indexes.c.table_name.in_(all_objects),
                    indexes.c.primary_key == sql.false(),
                )
            )
            .order_by(
                indexes.c.table_name,
                indexes.c.index_name,
                indexes.c.ordinal_position,
            )
        )
        if unique:
            s = s.where(indexes.c.non_unique != sql.true())

        if self._dictionary_access:
            s = s.add_columns(
                index_def.c.Data,
            ).outerjoin(
                index_def,
                sql.and_(
                    index_def.c.SqlName == indexes.c.index_name,
                    index_def.c.parent
                    == sql.select(tables.c.classname)
                    .where(
                        indexes.c.table_name == tables.c.table_name,
                        indexes.c.table_schema == tables.c.table_schema,
                    )
                    .scalar_subquery(),
                ),
            )
        else:
            s = s.add_columns(None)

        rs = connection.execute(s)

        flat_indexes = util.defaultdict(dict)
        default = ReflectionDefaults.indexes

        indexes = util.defaultdict(dict)
        for table_name in all_objects:
            indexes[(schema, table_name)] = default()

        for row in rs:
            (
                idxtable,
                idxname,
                colname,
                _,
                nuniq,
                _,
                include,
            ) = row

            if (schema, idxtable) not in indexes:
                continue

            indexrec = flat_indexes[(schema, idxtable, idxname)]
            if "name" not in indexrec:
                indexrec["name"] = self.normalize_name(idxname)
                indexrec["column_names"] = []
                if not unique:
                    indexrec["unique"] = not nuniq
                else:
                    indexrec["duplicates_index"] = idxname

            indexrec["column_names"].append(self.normalize_name(colname))
            include = include.split(",") if include else []
            if not unique or include:
                indexrec["include_columns"] = include
            if include:
                indexrec["dialect_options"] = {"iris_include": include}

        for schema, idxtable, idxname in flat_indexes:
            indexes[(schema, idxtable)].append(
                flat_indexes[(schema, idxtable, idxname)]
            )

        return indexes

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        data = self.get_multi_pk_constraint(
            connection,
            schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    def get_multi_pk_constraint(
        self,
        connection,
        schema,
        filter_names,
        scope,
        kind,
        **kw,
    ):
        schema_name = self.get_schema(schema)
        key_constraints = ischema.key_constraints
        constraints = ischema.constraints

        all_objects = self._get_all_objects(
            connection, schema, filter_names, scope, kind
        )
        if not all_objects:
            return util.defaultdict(list)

        s = (
            sql.select(
                key_constraints.c.table_name,
                key_constraints.c.constraint_name,
                key_constraints.c.column_name,
            )
            .join(
                constraints,
                sql.and_(
                    key_constraints.c.constraint_name == constraints.c.constraint_name,
                    key_constraints.c.table_schema == constraints.c.table_schema,
                ),
            )
            .where(
                sql.and_(
                    key_constraints.c.table_schema == str(schema_name),
                    key_constraints.c.table_name.in_(all_objects),
                    constraints.c.constraint_type == "PRIMARY KEY",
                )
            )
            .order_by(
                key_constraints.c.table_name,
                key_constraints.c.constraint_name,
                key_constraints.c.ordinal_position,
            )
        )

        rs = connection.execute(s)

        primary_keys = util.defaultdict(dict)
        default = ReflectionDefaults.pk_constraint

        constraint_name = None
        for row in rs:
            (
                table_name,
                name,
                colname,
            ) = row
            constraint_name = self.normalize_name(name)

            table_pk = primary_keys[(schema, table_name)]
            if not table_pk:
                table_pk["name"] = constraint_name
                table_pk["constrained_columns"] = [colname]
            else:
                table_pk["constrained_columns"].append(colname)

        return (
            (key, primary_keys[key] if key in primary_keys else default())
            for key in (
                (schema, self.normalize_name(obj_name)) for obj_name in all_objects
            )
        )

    def _value_or_raise(self, data, table, schema):
        table = self.normalize_name(str(table))
        try:
            return dict(data)[(schema, table)]
        except KeyError:
            raise exc.NoSuchTableError(
                f"{schema}.{table}" if schema else table
            ) from None

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        data = self.get_multi_unique_constraints(
            connection,
            schema=schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    def get_multi_unique_constraints(
        self,
        connection,
        schema,
        filter_names,
        scope,
        kind,
        **kw,
    ):
        return self.get_multi_indexes(
            connection, schema, filter_names, scope, kind, unique=True, **kw
        )

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        data = self.get_multi_foreign_keys(
            connection,
            schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    def get_multi_foreign_keys(
        self,
        connection,
        schema,
        filter_names,
        scope,
        kind,
        **kw,
    ):
        schema_name = self.get_schema(schema)
        ref_constraints = ischema.ref_constraints
        key_constraints = ischema.key_constraints
        key_constraints_ref = aliased(ischema.key_constraints)

        all_objects = self._get_all_objects(
            connection, schema, filter_names, scope, kind
        )
        if not all_objects:
            return util.defaultdict(list)

        s = (
            sql.select(
                key_constraints.c.table_name,
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
                    key_constraints.c.table_schema
                    == ref_constraints.c.constraint_schema,
                    key_constraints.c.constraint_name
                    == ref_constraints.c.constraint_name,
                ),
            )
            .join(
                key_constraints_ref,
                sql.and_(
                    key_constraints_ref.c.constraint_schema
                    == ref_constraints.c.unique_constraint_schema,
                    key_constraints_ref.c.constraint_name
                    == ref_constraints.c.unique_constraint_name,
                    key_constraints_ref.c.ordinal_position
                    == key_constraints.c.ordinal_position,
                ),
            )
            .where(
                sql.and_(
                    key_constraints.c.table_schema == str(schema_name),
                    key_constraints.c.table_name.in_(all_objects),
                )
            )
            .order_by(
                key_constraints.c.constraint_name,
                key_constraints.c.ordinal_position,
            )
        )

        rs = connection.execution_options(future_result=True).execute(s)

        fkeys = util.defaultdict(dict)

        for row in rs.mappings():
            table_name = row[key_constraints.c.table_name]
            rfknm = row[key_constraints.c.constraint_name]
            scol = row[key_constraints.c.column_name]
            rschema = row[key_constraints_ref.c.table_schema]
            rtbl = row[key_constraints_ref.c.table_name]
            rcol = row[key_constraints_ref.c.column_name]
            _ = row[ref_constraints.c.match_option]
            fkuprule = row[ref_constraints.c.update_rule]
            fkdelrule = row[ref_constraints.c.delete_rule]

            table_fkey = fkeys[(schema, table_name)]

            if rfknm not in table_fkey:
                table_fkey[rfknm] = fkey = {
                    "name": rfknm,
                    "constrained_columns": [],
                    "referred_schema": (
                        rschema if rschema != self.default_schema_name else None
                    ),
                    "referred_table": rtbl,
                    "referred_columns": [],
                    "options": {},
                }
            else:
                fkey = table_fkey[rfknm]

            if fkuprule != "NO ACTION":
                fkey["options"]["onupdate"] = fkuprule

            if fkdelrule != "NO ACTION":
                fkey["options"]["ondelete"] = fkdelrule

            if scol not in fkey["constrained_columns"]:
                fkey["constrained_columns"].append(scol)
            if rcol not in fkey["referred_columns"]:
                fkey["referred_columns"].append(rcol)

        default = ReflectionDefaults.foreign_keys

        return (
            (key, list(fkeys[key].values()) if key in fkeys else default())
            for key in (
                (schema, self.normalize_name(obj_name)) for obj_name in all_objects
            )
        )

    def get_columns(self, connection, table_name, schema=None, **kw):
        data = self.get_multi_columns(
            connection,
            schema,
            filter_names=[table_name],
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            **kw,
        )
        return self._value_or_raise(data, table_name, schema)

    def get_multi_columns(
        self,
        connection,
        schema,
        filter_names,
        scope,
        kind,
        **kw,
    ):
        schema_name = self.get_schema(schema)
        tables = ischema.tables
        columns = ischema.columns
        property = ischema.property_definition

        all_objects = self._get_all_objects(
            connection, schema, filter_names, scope, kind
        )
        if not all_objects:
            return util.defaultdict(list)

        s = (
            sql.select(
                columns.c.table_name,
                columns.c.column_name,
                columns.c.data_type,
                columns.c.is_nullable,
                columns.c.character_maximum_length,
                columns.c.numeric_precision,
                columns.c.numeric_scale,
                columns.c.column_default,
                columns.c.collation_name,
                columns.c.auto_increment,
            )
            .select_from(columns)
            .where(
                columns.c.table_schema == str(schema_name),
            )
            .order_by(columns.c.ordinal_position)
        )
        if all_objects:
            s = s.where(columns.c.table_name.in_(all_objects))

        if self._dictionary_access:
            s = s.add_columns(
                property.c.SqlComputeCode,
                property.c.Calculated,
                property.c.Transient,
            ).outerjoin(
                property,
                sql.and_(
                    sql.or_(
                        property.c.Name == columns.c.column_name,
                        property.c.SqlFieldName == columns.c.column_name,
                    ),
                    property.c.parent
                    == sql.select(tables.c.classname)
                    .where(
                        columns.c.table_name == tables.c.table_name,
                        columns.c.table_schema == tables.c.table_schema,
                    )
                    .scalar_subquery(),
                ),
            )

        c = connection.execution_options(future_result=True).execute(s)

        cols = util.defaultdict(list)

        for row in c.mappings():
            table_name = row[columns.c.table_name]
            name = row[columns.c.column_name]
            type_ = row[columns.c.data_type].upper()
            nullable = row[columns.c.is_nullable]
            charlen = row[columns.c.character_maximum_length]
            numericprec = row[columns.c.numeric_precision]
            numericscale = row[columns.c.numeric_scale]
            default = row[columns.c.column_default]
            collation = row[columns.c.collation_name]
            autoincrement = row[columns.c.auto_increment]
            sqlComputeCode = calculated = transient = None
            if self._dictionary_access:
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
                    kwargs["length"] = 0
                else:
                    try:
                        kwargs["length"] = int(charlen)
                    except ValueError:
                        kwargs["length"] = 0
                if collation:
                    kwargs["collation"] = collation
            if coltype is None:
                util.warn("Did not recognize type '%s' of column '%s'" % (type_, name))
                coltype = sqltypes.NULLTYPE
            elif coltype is VARCHAR and charlen == 1:
                # VARCHAR(1) as CHAR
                coltype = CHAR
            else:
                if issubclass(coltype, sqltypes.Numeric):
                    kwargs["precision"] = int(numericprec)

                    if not issubclass(coltype, sqltypes.Float):
                        kwargs["scale"] = int(numericscale)

                coltype = coltype(**kwargs)

            default = "" if default == "$c(0)" else default
            if default and default.startswith('"'):
                default = "'%s'" % (default[1:-1].replace("'", "''"),)

            cdict = {
                "name": name,
                "type": coltype,
                "nullable": nullable,
                "default": default,
                "autoincrement": autoincrement,
                # "comment": description,
            }
            if sqlComputeCode and "set {*} = " in sqlComputeCode.lower():
                sqltext = sqlComputeCode
                sqltext = sqltext.split(" = ")[1]
                sqltext = re.sub(r"{(\b\w+\b)}", r"\g<1>", sqltext)
                persisted = not calculated and not transient
                cdict["computed"] = {
                    "sqltext": sqltext,
                    "persisted": persisted,
                }
            cols[(schema, table_name)].append(cdict)

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
            sql.select(views.c.view_definition).where(
                views.c.table_schema == str(schema_name),
                views.c.table_name == str(view_name),
            )
        ).scalar()

        if view_def:
            return view_def
        raise exc.NoSuchTableError(f"{schema}.{view_name}")

    def normalize_name(self, name):
        if self.identifier_preparer._requires_quotes(name):
            return quoted_name(name, quote=True)
        return name
