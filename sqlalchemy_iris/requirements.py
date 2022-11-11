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

