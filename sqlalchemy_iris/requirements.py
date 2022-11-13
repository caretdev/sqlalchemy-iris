from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
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
