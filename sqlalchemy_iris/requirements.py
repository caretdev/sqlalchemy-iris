from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing.exclusions import against
from sqlalchemy.testing.exclusions import only_on

try:
    from alembic.testing.requirements import SuiteRequirements as AlembicRequirements
except:  # noqa
    from sqlalchemy.testing.requirements import Requirements as BaseRequirements

    class AlembicRequirements(BaseRequirements):
        pass


from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements, AlembicRequirements):

    @property
    def community_driver(self):
        return exclusions.only_if(
            lambda config: not config.db.dialect.driver == "intersystems",
            "Only on community driver"
        )

    @property
    def intersystems_driver(self):
        return exclusions.only_if(
            lambda config: config.db.dialect.driver == "intersystems",
            "InterSystems official driver"
        )

    @property
    def array_type(self):
        return exclusions.closed()

    @property
    def table_ddl_if_exists(self):
        return exclusions.open()

    @property
    def uuid_data_type(self):
        return exclusions.open()

    @property
    def check_constraints(self):
        """Target database must support check constraints."""

        return exclusions.closed()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()

    @property
    def supports_distinct_on(self):
        """If a backend supports the DISTINCT ON in a select"""
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        return exclusions.open()

    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return exclusions.open()

    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return exclusions.open()

    @property
    def reflect_table_options(self):
        return exclusions.open()

    @property
    def comment_reflection(self):
        return exclusions.open()

    @property
    def insert_returning(self):
        return exclusions.skip_if(
            lambda config: not config.db.dialect.insert_returning,
            "driver doesn't support insert returning",
        )

    @property
    def computed_columns(self):
        "Supports computed columns"
        return exclusions.open()

    @property
    def computed_columns_stored(self):
        "Supports computed columns with `persisted=True`"
        return exclusions.open()

    @property
    def computed_columns_virtual(self):
        "Supports computed columns with `persisted=False`"
        return exclusions.open()

    @property
    def computed_columns_default_persisted(self):
        """If the default persistence is virtual or stored when `persisted`
        is omitted"""
        return exclusions.open()

    @property
    def computed_columns_reflect_persisted(self):
        """If persistence information is returned by the reflection of
        computed columns"""
        return exclusions.open()

    @property
    def two_phase_transactions(self):
        """Target database must support two-phase transactions."""

        return exclusions.closed()

    @property
    def binary_comparisons(self):
        """target database/driver can allow BLOB/BINARY fields to be compared
        against a bound parameter value.
        """

        return exclusions.closed()

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.
        """

        return exclusions.open()

    @property
    def foreign_key_constraint_option_reflection_ondelete(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_restrict(self):
        return exclusions.closed()

    @property
    def fk_constraint_option_reflection_ondelete_noaction(self):
        return exclusions.open()

    @property
    def foreign_key_constraint_option_reflection_onupdate(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_onupdate_restrict(self):
        return exclusions.closed()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return exclusions.closed()

    @property
    def symbol_names_w_double_quote(self):
        """Target driver can create tables with a name like 'some " table'"""
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        return exclusions.open()

    @property
    def index_reflects_included_columns(self):
        return exclusions.open()

    @property
    def intersect(self):
        """Target database must support INTERSECT or equivalent."""
        return exclusions.closed()

    @property
    def except_(self):
        """Target database must support EXCEPT or equivalent (i.e. MINUS)."""
        return exclusions.closed()

    @property
    def boolean_col_expressions(self):
        """Target database must support boolean expressions as columns"""

        return exclusions.closed()

    @property
    def memory_process_intensive(self):
        """Driver is able to handle the memory tests which run in a subprocess
        and iterate through hundreds of connections

        """
        return exclusions.closed()

    @property
    def ctes(self):
        """Target database supports CTEs"""
        return exclusions.skip_if(
            lambda config: not config.db.dialect.supports_cte,
            "driver doesn't support CTEs",
        )

    @property
    def ctes_with_update_delete(self):
        """target database supports CTES that ride on top of a normal UPDATE
        or DELETE statement which refers to the CTE in a correlated subquery.

        """

        return exclusions.open()

    @property
    def ctes_on_dml(self):
        """target database supports CTES which consist of INSERT, UPDATE
        or DELETE *within* the CTE, e.g. WITH x AS (UPDATE....)"""

        return exclusions.open()

    @property
    def autocommit(self):
        """target dialect supports 'AUTOCOMMIT' as an isolation_level"""
        return exclusions.open()

    def get_isolation_levels(self, config):
        levels = set(config.db.dialect._isolation_lookup)

        default = "READ UNCOMMITTED"
        levels.add("AUTOCOMMIT")

        return {"default": default, "supported": levels}

    @property
    def regexp_match(self):
        """backend supports the regexp_match operator."""
        # InterSystems use own format for %MATCHES and %PATTERN, it does not support Regular Expressions
        return exclusions.closed()

    @property
    def unique_constraints_reflect_as_index(self):
        """Target database reflects unique constraints as indexes."""

        return exclusions.open()

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""
        return exclusions.open()

    @property
    def unique_index_reflect_as_unique_constraints(self):
        """Target database reflects unique indexes as unique constrains."""

        return exclusions.open()

    # alembic

    @property
    def fk_onupdate_restrict(self):
        return exclusions.closed()

    @property
    def fk_ondelete_restrict(self):
        return exclusions.closed()

    def _iris_vector(self, config):
        if not against(config, "iris >= 2024.1"):
            return False
        else:
            return config.db.dialect.supports_vectors

    @property
    def iris_vector(self):
        return only_on(lambda config: self._iris_vector(config))

    @property
    def index_ddl_if_exists(self):
        """target platform supports IF NOT EXISTS / IF EXISTS for indexes."""

        return exclusions.closed()

    @property
    def foreign_keys(self):
        """Target database must support foreign keys."""

        return exclusions.open()

    @property
    def foreign_keys_reflect_as_index(self):
        """Target database creates an index that's reflected for
        foreign keys."""

        return exclusions.closed()

    @property
    def table_value_constructor(self):
        """Database / dialect supports a query like::

             SELECT * FROM VALUES ( (c1, c2), (c1, c2), ...)
             AS some_table(col1, col2)

        SQLAlchemy generates this with the :func:`_sql.values` function.

        """
        return exclusions.closed()

    @property
    def standard_cursor_sql(self):
        """Target database passes SQL-92 style statements to cursor.execute()
        when a statement like select() or insert() is run.

        A very small portion of dialect-level tests will ensure that certain
        conditions are present in SQL strings, and these tests use very basic
        SQL that will work on any SQL-like platform in order to assert results.

        It's normally a given for any pep-249 DBAPI that a statement like
        "SELECT id, name FROM table WHERE some_table.id=5" will work.
        However, there are dialects that don't actually produce SQL Strings
        and instead may work with symbolic objects instead, or dialects that
        aren't working with SQL, so for those this requirement can be marked
        as excluded.

        """

        return exclusions.open()

    @property
    def on_update_cascade(self):
        """target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.open()

    @property
    def non_updating_cascade(self):
        """target database must *not* support ON UPDATE..CASCADE behavior in
        foreign keys."""
        return exclusions.closed()

    @property
    def deferrable_fks(self):
        return exclusions.closed()

    @property
    def on_update_or_deferrable_fks(self):
        # TODO: exclusions should be composable,
        # somehow only_if([x, y]) isn't working here, negation/conjunctions
        # getting confused.
        return exclusions.only_if(
            lambda: self.on_update_cascade.enabled
            or self.deferrable_fks.enabled
        )

    @property
    def self_referential_foreign_keys(self):
        """Target database must support self-referential foreign keys."""

        return exclusions.open()

    @property
    def foreign_key_ddl(self):
        """Target database must support the DDL phrases for FOREIGN KEY."""

        return exclusions.open()

    @property
    def named_constraints(self):
        """target database must support names for constraints."""

        return exclusions.open()

    @property
    def implicitly_named_constraints(self):
        """target database must apply names to unnamed constraints."""

        return exclusions.open()

    @property
    def unusual_column_name_characters(self):
        """target database allows column names that have unusual characters
        in them, such as dots, spaces, slashes, or percent signs.

        The column names are as always in such a case quoted, however the
        DB still needs to support those characters in the name somehow.

        """
        return exclusions.open()

    @property
    def subqueries(self):
        """Target database must support subqueries."""

        return exclusions.open()

    @property
    def offset(self):
        """target database can render OFFSET, or an equivalent, in a
        SELECT.
        """

        return exclusions.open()

    @property
    def bound_limit_offset(self):
        """target database can render LIMIT and/or OFFSET using a bound
        parameter
        """

        return exclusions.open()

    @property
    def sql_expression_limit_offset(self):
        """target database can render LIMIT and/or OFFSET with a complete
        SQL expression, such as one that uses the addition operator.
        parameter
        """
        # IRIS does not support expressions in LIMIT/OFFSET
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when LIMIT/OFFSET is specifically present.

        E.g. (SELECT ...) UNION (SELECT ..)
        """
        return exclusions.open()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when OFFSET/LIMIT is specifically not present.

        E.g. (SELECT ... LIMIT ..) UNION (SELECT .. OFFSET ..)
        """
        return exclusions.open()

    @property
    def nullable_booleans(self):
        """Target database allows boolean columns to store NULL."""

        return exclusions.open()

    @property
    def nullsordering(self):
        """Target backends that support nulls ordering."""

        return exclusions.closed()

    @property
    def standalone_binds(self):
        """target database/driver supports bound parameters as column
        expressions without being in the context of a typed column.
        """
        return exclusions.open()

    @property
    def standalone_null_binds_whereclause(self):
        """target database/driver supports bound parameters with NULL in the
        WHERE clause, in situations where it has to be typed.

        """
        return exclusions.open()

    @property
    def window_functions(self):
        """Target database must support window functions."""
        return exclusions.closed()

    @property
    def autoincrement_insert(self):
        """target platform generates new surrogate integer primary key values
        when insert() is executed, excluding the pk column."""

        return exclusions.open()

    @property
    def fetch_rows_post_commit(self):
        """target platform will allow cursor.fetchone() to proceed after a
        COMMIT.

        Typically this refers to an INSERT statement with RETURNING which
        is invoked within "autocommit".   If the row can be returned
        after the autocommit, then this rule can be open.

        """

        return exclusions.open()

    @property
    def group_by_complex_expression(self):
        """target platform supports SQL expressions in GROUP BY

        e.g.

        SELECT x + y AS somelabel FROM table GROUP BY x + y

        """

        return exclusions.open()

    @property
    def sane_rowcount(self):
        return exclusions.skip_if(
            lambda config: not config.db.dialect.supports_sane_rowcount,
            "driver doesn't support 'sane' rowcount",
        )

    @property
    def sane_multi_rowcount(self):
        return exclusions.fails_if(
            lambda config: not config.db.dialect.supports_sane_multi_rowcount,
            "driver %(driver)s %(doesnt_support)s 'sane' multi row count",
        )

    @property
    def sane_rowcount_w_returning(self):
        return exclusions.fails_if(
            lambda config: not (
                config.db.dialect.supports_sane_rowcount_returning
            ),
            "driver doesn't support 'sane' rowcount when returning is on",
        )

    @property
    def empty_inserts(self):
        """target platform supports INSERT with no values, i.e.
        INSERT DEFAULT VALUES or equivalent."""

        return exclusions.only_if(
            lambda config: config.db.dialect.supports_empty_insert
            or config.db.dialect.supports_default_values
            or config.db.dialect.supports_default_metavalue,
            "empty inserts not supported",
        )

    @property
    def empty_inserts_executemany(self):
        """target platform supports INSERT with no values, i.e.
        INSERT DEFAULT VALUES or equivalent, within executemany()"""

        return self.empty_inserts

    @property
    def insert_from_select(self):
        """target platform supports INSERT from a SELECT."""

        return exclusions.open()

    @property
    def delete_returning(self):
        """target platform supports DELETE ... RETURNING."""

        return exclusions.only_if(
            lambda config: config.db.dialect.delete_returning,
            "%(database)s %(does_support)s 'DELETE ... RETURNING'",
        )

    @property
    def update_returning(self):
        """target platform supports UPDATE ... RETURNING."""

        return exclusions.only_if(
            lambda config: config.db.dialect.update_returning,
            "%(database)s %(does_support)s 'UPDATE ... RETURNING'",
        )

    @property
    def insert_executemany_returning(self):
        """target platform supports RETURNING when INSERT is used with
        executemany(), e.g. multiple parameter sets, indicating
        as many rows come back as do parameter sets were passed.

        """

        return exclusions.only_if(
            lambda config: config.db.dialect.insert_executemany_returning,
            "%(database)s %(does_support)s 'RETURNING of "
            "multiple rows with INSERT executemany'",
        )

    @property
    def insertmanyvalues(self):
        return exclusions.only_if(
            lambda config: config.db.dialect.supports_multivalues_insert
            and config.db.dialect.insert_returning
            and config.db.dialect.use_insertmanyvalues,
            "%(database)s %(does_support)s 'insertmanyvalues functionality",
        )

    @property
    def tuple_in(self):
        """Target platform supports the syntax
        "(x, y) IN ((x1, y1), (x2, y2), ...)"
        """

        return exclusions.closed()

    @property
    def tuple_in_w_empty(self):
        """Target platform tuple IN w/ empty set"""
        return self.tuple_in

    @property
    def duplicate_names_in_cursor_description(self):
        """target platform supports a SELECT statement that has
        the same name repeated more than once in the columns list."""

        return exclusions.open()

    @property
    def denormalized_names(self):
        """Target database must have 'denormalized', i.e.
        UPPERCASE as case insensitive names."""

        return exclusions.skip_if(
            lambda config: not config.db.dialect.requires_name_normalize,
            "Backend does not require denormalized names.",
        )

    @property
    def multivalues_inserts(self):
        """target database must support multiple VALUES clauses in an
        INSERT statement."""

        return exclusions.skip_if(
            lambda config: not config.db.dialect.supports_multivalues_insert,
            "Backend does not support multirow inserts.",
        )

    @property
    def implements_get_lastrowid(self):
        """target dialect implements the executioncontext.get_lastrowid()
        method without reliance on RETURNING.

        """
        return exclusions.open()

    @property
    def arraysize(self):
        """dialect includes the required pep-249 attribute
        ``cursor.arraysize``"""

        return exclusions.open()

    @property
    def emulated_lastrowid(self):
        """target dialect retrieves cursor.lastrowid, or fetches
        from a database-side function after an insert() construct executes,
        within the get_lastrowid() method.

        Only dialects that "pre-execute", or need RETURNING to get last
        inserted id, would return closed/fail/skip for this.

        """
        return exclusions.closed()

    @property
    def emulated_lastrowid_even_with_sequences(self):
        """target dialect retrieves cursor.lastrowid or an equivalent
        after an insert() construct executes, even if the table has a
        Sequence on it.

        """
        return exclusions.closed()

    @property
    def dbapi_lastrowid(self):
        """target platform includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return exclusions.closed()

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return only_on(lambda config: config.db.dialect.supports_schemas)

    @property
    def cross_schema_fk_reflection(self):
        """target system must support reflection of inter-schema
        foreign keys"""
        return exclusions.closed()

    @property
    def foreign_key_constraint_name_reflection(self):
        """Target supports reflection of FOREIGN KEY constraints and
        will return the name of the constraint that was used in the
        "CONSTRAINT <name> FOREIGN KEY" DDL.
        """
        return exclusions.closed()

    @property
    def implicit_default_schema(self):
        """target system has a strong concept of 'default' schema that can
        be referred to implicitly.
        """
        return exclusions.closed()

    @property
    def default_schema_name_switch(self):
        """target dialect implements provisioning module including
        set_default_schema_on_connection"""

        return exclusions.closed()

    @property
    def server_side_cursors(self):
        """Target dialect must support server side cursors."""

        return exclusions.only_if(
            [lambda config: config.db.dialect.supports_server_side_cursors],
            "no server side cursors support",
        )

    @property
    def sequences(self):
        """Target database must support SEQUENCEs."""

        return exclusions.only_if(
            [lambda config: config.db.dialect.supports_sequences],
            "no sequence support",
        )

    @property
    def no_sequences(self):
        """the opposite of "sequences", DB does not support sequences at
        all."""

        return exclusions.NotPredicate(self.sequences)

    @property
    def sequences_optional(self):
        """Target database supports sequences, but also optionally
        as a means of generating new PK values."""

        return exclusions.only_if(
            [
                lambda config: config.db.dialect.supports_sequences
                and config.db.dialect.sequences_optional
            ],
            "no sequence support, or sequences not optional",
        )

    @property
    def supports_lastrowid(self):
        """target database / driver supports cursor.lastrowid as a means
        of retrieving the last inserted primary key value.

        note that if the target DB supports sequences also, this is still
        assumed to work.  This is a new use case brought on by MariaDB 10.3.

        """
        return exclusions.only_if(
            [lambda config: config.db.dialect.postfetch_lastrowid]
        )

    @property
    def no_lastrowid_support(self):
        """the opposite of supports_lastrowid"""
        return exclusions.only_if(
            [lambda config: not config.db.dialect.postfetch_lastrowid]
        )

    @property
    def table_reflection(self):
        """target database has general support for table reflection"""
        return exclusions.open()

    @property
    def reflect_tables_no_columns(self):
        """target database supports creation and reflection of tables with no
        columns, or at least tables that seem to have no columns."""

        return exclusions.closed()

    @property
    def comment_reflection_full_unicode(self):
        """Indicates if the database support table comment reflection in the
        full unicode range, including emoji etc.
        """
        return exclusions.closed()

    @property
    def constraint_comment_reflection(self):
        """indicates if the database support comments on constraints
        and their reflection"""
        return exclusions.closed()

    @property
    def view_column_reflection(self):
        """target database must support retrieval of the columns in a view,
        similarly to how a table is inspected.

        This does not include the full CREATE VIEW definition.

        """
        return self.views

    @property
    def view_reflection(self):
        """target database must support inspection of the full CREATE VIEW
        definition."""
        return self.views

    @property
    def schema_reflection(self):
        return self.schemas

    @property
    def schema_create_delete(self):
        """target database supports schema create and dropped with
        'CREATE SCHEMA' and 'DROP SCHEMA'"""
        return exclusions.closed()

    @property
    def primary_key_constraint_reflection(self):
        return exclusions.open()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.open()

    @property
    def temp_table_reflection(self):
        return exclusions.open()

    @property
    def temp_table_reflect_indexes(self):
        return self.temp_table_reflection

    @property
    def has_temp_table(self):
        """target dialect supports checking a single temp table name"""
        return exclusions.closed()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.open()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return exclusions.closed()

    @property
    def index_reflection(self):
        return exclusions.open()

    @property
    def indexes_with_ascdesc(self):
        """target database supports CREATE INDEX with per-column ASC/DESC."""
        return exclusions.open()

    @property
    def reflect_indexes_with_ascdesc(self):
        """target database supports reflecting INDEX with per-column
        ASC/DESC."""
        return exclusions.open()

    @property
    def reflect_indexes_with_ascdesc_as_expression(self):
        """target database supports reflecting INDEX with per-column
        ASC/DESC but reflects them as expressions."""
        return exclusions.closed()

    @property
    def indexes_with_expressions(self):
        """target database supports CREATE INDEX against SQL expressions."""
        return exclusions.closed()

    @property
    def reflect_indexes_with_expressions(self):
        """target database supports reflection of indexes with
        SQL expressions."""
        return exclusions.closed()

    @property
    def inline_check_constraint_reflection(self):
        """target dialect supports reflection of inline check constraints"""
        return exclusions.closed()

    @property
    def check_constraint_reflection(self):
        """target dialect supports reflection of check constraints"""
        return exclusions.closed()

    @property
    def duplicate_key_raises_integrity_error(self):
        """target dialect raises IntegrityError when reporting an INSERT
        with a primary key violation.  (hint: it should)

        """
        return exclusions.open()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.open()

    @property
    def unicode_data_no_special_types(self):
        """Target database/dialect can receive / deliver / compare data with
        non-ASCII characters in plain VARCHAR, TEXT columns, without the need
        for special "national" datatypes like NVARCHAR or similar.

        """
        return exclusions.open()

    @property
    def unicode_data(self):
        """Target database/dialect must support Python unicode objects with
        non-ASCII characters represented, delivered as bound parameters
        as well as in result rows.

        """
        return exclusions.open()

    @property
    def unicode_ddl(self):
        """Target driver must support some degree of non-ascii symbol
        names.
        """
        return exclusions.open()

    @property
    def datetime_interval(self):
        """target dialect supports rendering of a datetime.timedelta as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """
        return exclusions.closed()

    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """
        # works stable only on Community driver
        return self.community_driver

    @property
    def datetime(self):
        """target dialect supports representation of Python
        datetime.datetime() objects."""

        return exclusions.open()

    @property
    def datetime_timezone(self):
        """target dialect supports representation of Python
        datetime.datetime() with tzinfo with DateTime(timezone=True)."""

        return exclusions.closed()

    @property
    def time_timezone(self):
        """target dialect supports representation of Python
        datetime.time() with tzinfo with Time(timezone=True)."""

        return exclusions.closed()

    @property
    def date_implicit_bound(self):
        """target dialect when given a date object will bind it such
        that the database server knows the object is a date, and not
        a plain string.

        """
        return exclusions.open()

    @property
    def time_implicit_bound(self):
        """target dialect when given a time object will bind it such
        that the database server knows the object is a time, and not
        a plain string.

        """
        return exclusions.open()

    @property
    def datetime_implicit_bound(self):
        """target dialect when given a datetime object will bind it such
        that the database server knows the object is a datetime, and not
        a plain string.

        """
        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.open()

    @property
    def timestamp_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""
        return exclusions.closed()

    @property
    def timestamp_microseconds_implicit_bound(self):
        """target dialect when given a datetime object which also includes
        a microseconds portion when using the TIMESTAMP data type
        will bind it such that the database server knows
        the object is a datetime with microseconds, and not a plain string.

        """
        return self.timestamp_microseconds

    @property
    def date(self):
        """target dialect supports representation of Python
        datetime.date() objects."""

        return exclusions.open()

    @property
    def date_coerces_from_datetime(self):
        """target dialect accepts a datetime object as the target
        of a date column."""

        return exclusions.open()

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return exclusions.open()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.open()

    @property
    def isolation_level(self):
        """target dialect supports general isolation level settings.

        Note that this requirement, when enabled, also requires that
        the get_isolation_levels() method be implemented.

        """
        return exclusions.open()

    @property
    def get_isolation_level_values(self):
        """target dialect supports the
        :meth:`_engine.Dialect.get_isolation_level_values`
        method added in SQLAlchemy 2.0.

        """

        def go(config):
            with config.db.connect() as conn:
                try:
                    conn.dialect.get_isolation_level_values(
                        conn.connection.dbapi_connection
                    )
                except NotImplementedError:
                    return False
                else:
                    return True

        return exclusions.only_if(go)

    @property
    def json_type(self):
        """target platform implements a native JSON type."""

        return exclusions.closed()

    @property
    def json_array_indexes(self):
        """target platform supports numeric array indexes
        within a JSON structure"""

        return self.json_type

    @property
    def json_index_supplementary_unicode_element(self):
        return exclusions.open()

    @property
    def legacy_unconditional_json_extract(self):
        """Backend has a JSON_EXTRACT or similar function that returns a
        valid JSON string in all cases.

        Used to test a legacy feature and is not needed.

        """
        return exclusions.closed()

    @property
    def precision_numerics_general(self):
        """target backend has general support for moderately high-precision
        numerics."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        return exclusions.closed()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""
        return exclusions.open()

    @property
    def cast_precision_numerics_many_significant_digits(self):
        """same as precision_numerics_many_significant_digits but within the
        context of a CAST statement

        """
        return self.precision_numerics_many_significant_digits

    @property
    def implicit_decimal_binds(self):
        """target backend will return a selected Decimal as a Decimal, not
        a string.
        """

        return exclusions.open()

    @property
    def numeric_received_as_decimal_untyped(self):
        """target backend will return result columns that are explicitly
        against NUMERIC or similar precision-numeric datatypes (not including
        FLOAT or INT types) as Python Decimal objects, and not as floats
        or ints, including when no SQLAlchemy-side typing information is
        associated with the statement (e.g. such as a raw SQL string).

        This should be enabled if either the DBAPI itself returns Decimal
        objects, or if the dialect has set up DBAPI-specific return type
        handlers such that Decimal objects come back automatically.

        """
        return exclusions.open()

    @property
    def nested_aggregates(self):
        """target database can select an aggregate from a subquery that's
        also using an aggregate

        """
        return exclusions.open()

    @property
    def recursive_fk_cascade(self):
        """target database must support ON DELETE CASCADE on a self-referential
        foreign key

        """
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.closed()

    @property
    def infinity_floats(self):
        """The Float type can persist and load float('inf'), float('-inf')."""

        return exclusions.closed()

    @property
    def float_or_double_precision_behaves_generically(self):
        return exclusions.closed()

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type.

        """
        return exclusions.open()

    @property
    def literal_float_coercion(self):
        """target backend will return the exact float value 15.7563
        with only four significant digits from this statement:

        SELECT :param

        where :param is the Python float 15.7563

        i.e. it does not return 15.75629997253418

        """
        return exclusions.open()

    @property
    def floats_to_four_decimals(self):
        """target backend can return a floating-point number with four
        significant digits (such as 15.7563) accurately
        (i.e. without FP inaccuracies, such as 15.75629997253418).

        """
        return exclusions.open()

    @property
    def fetch_null_from_numeric(self):
        """target backend doesn't crash when you try to select a NUMERIC
        value that has a value of NULL.

        Added to support Pyodbc bug #351.
        """

        return exclusions.open()

    @property
    def float_is_numeric(self):
        """target backend uses Numeric for Float/Dual"""

        return exclusions.open()

    @property
    def text_type(self):
        """Target database must support an unbounded Text() "
        "type such as TEXT or CLOB"""

        return exclusions.open()

    @property
    def empty_strings_varchar(self):
        """target database can persist/return an empty string with a
        varchar.

        """
        return exclusions.open()

    @property
    def empty_strings_text(self):
        """target database can persist/return an empty string with an
        unbounded text."""

        return exclusions.open()

    @property
    def expressions_against_unbounded_text(self):
        """target database supports use of an unbounded textual field in a
        WHERE clause."""

        return exclusions.open()

    @property
    def selectone(self):
        """target driver must support the literal statement 'select 1'"""
        return exclusions.open()

    @property
    def savepoints(self):
        """Target database must support savepoints."""

        return exclusions.closed()

    @property
    def update_from(self):
        """Target must support UPDATE..FROM syntax"""
        return exclusions.closed()

    @property
    def delete_from(self):
        """Target must support DELETE FROM..FROM or DELETE..USING syntax"""
        return exclusions.closed()

    @property
    def update_where_target_in_subquery(self):
        """Target must support UPDATE (or DELETE) where the same table is
        present in a subquery in the WHERE clause.
        """
        return exclusions.open()

    @property
    def mod_operator_as_percent_sign(self):
        """target database must use a plain percent '%' as the 'modulus'
        operator."""
        return exclusions.closed()

    @property
    def percent_schema_names(self):
        """target backend supports weird identifiers with percent signs
        in them, e.g. 'some % column'.

        this is a very weird use case but often has problems because of
        DBAPIs that use python formatting.  It's not a critical use
        case either.

        """
        return exclusions.closed()

    @property
    def order_by_col_from_union(self):
        """target database supports ordering by a column from a SELECT
        inside of a UNION

        E.g.  (SELECT id, ...) UNION (SELECT id, ...) ORDER BY id

        """
        return exclusions.open()

    @property
    def order_by_collation(self):
        def check(config):
            try:
                self.get_order_by_collation(config)
                return False
            except NotImplementedError:
                return True

        return exclusions.skip_if(check)

    def get_order_by_collation(self, config):
        raise NotImplementedError()

    @property
    def unicode_connections(self):
        """Target driver must support non-ASCII characters being passed at
        all.
        """
        return exclusions.open()

    @property
    def graceful_disconnects(self):
        """Target driver must raise a DBAPI-level exception, such as
        InterfaceError, when the underlying connection has been closed
        and the execute() method is called.
        """
        return exclusions.open()

    @property
    def independent_connections(self):
        """
        Target must support simultaneous, independent database connections.
        """
        return exclusions.open()

    @property
    def independent_readonly_connections(self):
        """
        Target must support simultaneous, independent database connections
        that will be used in a readonly fashion.

        """
        return exclusions.open()

    @property
    def async_dialect(self):
        """dialect makes use of await_() to invoke operations on the DBAPI."""

        return exclusions.closed()

    @property
    def supports_is_distinct_from(self):
        """Supports some form of "x IS [NOT] DISTINCT FROM y" construct.
        Different dialects will implement their own flavour

        .. seealso::

            :meth:`.ColumnOperators.is_distinct_from`

        """
        return exclusions.skip_if(
            lambda config: not config.db.dialect.supports_is_distinct_from,
            "driver doesn't support an IS DISTINCT FROM construct",
        )

    @property
    def identity_columns(self):
        """If a backend supports GENERATED { ALWAYS | BY DEFAULT }
        AS IDENTITY"""
        return exclusions.closed()

    @property
    def identity_columns_standard(self):
        """If a backend supports GENERATED { ALWAYS | BY DEFAULT }
        AS IDENTITY with a standard syntax.
        """
        return exclusions.closed()

    @property
    def regexp_replace(self):
        """backend supports the regexp_replace operator."""
        return exclusions.closed()

    @property
    def fetch_first(self):
        """backend supports the fetch first clause."""
        return exclusions.open()

    @property
    def fetch_percent(self):
        """backend supports the fetch first clause with percent."""
        return exclusions.closed()

    @property
    def fetch_ties(self):
        """backend supports the fetch first clause with ties."""
        return exclusions.closed()

    @property
    def fetch_no_order_by(self):
        """backend supports the fetch first without order by"""
        return exclusions.open()

    @property
    def fetch_offset_with_options(self):
        """backend supports the offset when using fetch first with percent
        or ties.
        """
        return exclusions.open()

    @property
    def fetch_expression(self):
        """backend supports fetch / offset with expression in them, like

        SELECT * FROM some_table
        OFFSET 1 + 1 ROWS FETCH FIRST 1 + 1 ROWS ONLY
        """
        return exclusions.closed()

    @property
    def autoincrement_without_sequence(self):
        """If autoincrement=True on a column does not require an explicit
        sequence.
        """
        return exclusions.open()

    @property
    def generic_classes(self):
        "If X[Y] can be implemented with ``__class_getitem__``. py3.7+"
        return exclusions.open()

    @property
    def json_deserializer_binary(self):
        "indicates if the json_deserializer function is called with bytes"
        return exclusions.closed()

    @property
    def materialized_views(self):
        """Target database must support MATERIALIZED VIEWs."""
        return exclusions.closed()

    @property
    def materialized_views_reflect_pk(self):
        """Target database reflect MATERIALIZED VIEWs pks."""
        return exclusions.closed()

    @property
    def supports_bitwise_or(self):
        """Target database supports bitwise or"""
        return exclusions.closed()

    @property
    def supports_bitwise_and(self):
        """Target database supports bitwise and"""
        return exclusions.closed()

    @property
    def supports_bitwise_not(self):
        """Target database supports bitwise not"""
        return exclusions.closed()

    @property
    def supports_bitwise_xor(self):
        """Target database supports bitwise xor"""
        return exclusions.closed()

    @property
    def supports_bitwise_shift(self):
        """Target database supports bitwise left or right shift"""
        return exclusions.closed()
