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
    def array_type(self):
        return exclusions.closed()

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
    def order_by_label_with_expression(self):
        """target backend supports ORDER BY a column label within an
        expression.

        Basically this::

            select data as foo from test order by foo || 'bar'

        Lots of databases including PostgreSQL don't support this,
        so this is off by default.

        """
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

        return exclusions.open()

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

        default = "READ COMMITTED"
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
