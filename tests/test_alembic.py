try:
    import alembic  # noqa
except:  # noqa
    pass
else:
    from sqlalchemy import MetaData
    from sqlalchemy import Table
    from sqlalchemy import inspect
    from sqlalchemy import ForeignKey
    from sqlalchemy import Column
    from sqlalchemy import Integer
    from sqlalchemy import text
    from sqlalchemy.types import Text
    from sqlalchemy.types import LargeBinary

    from alembic import op
    from alembic.testing import fixture
    from alembic.testing import combinations
    from alembic.testing import eq_
    from alembic.testing.fixtures import TestBase
    from alembic.testing.fixtures import op_fixture
    from alembic.testing.suite._autogen_fixtures import AutogenFixtureTest

    from alembic.testing.suite.test_op import (
        BackendAlterColumnTest as _BackendAlterColumnTest,
    )
    from alembic.testing.suite.test_autogen_diffs import (
        AutoincrementTest as _AutoincrementTest,
    )
    from alembic.testing.suite import *  # noqa

    class BackendAlterColumnTest(_BackendAlterColumnTest):
        def test_rename_column(self):
            # IRIS Uppercases new names
            self._run_alter_col({}, {"name": "NEWNAME"})

    class AutoincrementTest(_AutoincrementTest):
        # pk don't change type
        def test_alter_column_autoincrement_pk_implicit_true(self):
            pass

        def test_alter_column_autoincrement_pk_explicit_true(self):
            pass

    @combinations(
        (None,),
        ("test",),
        argnames="schema",
        id_="s",
    )
    class RoundTripTest(TestBase):
        @fixture
        def tables(self, connection):
            self.meta = MetaData()
            self.meta.schema = self.schema
            self.tbl_other = Table(
                "other", self.meta, Column("oid", Integer, primary_key=True)
            )
            self.tbl = Table(
                "round_trip_table",
                self.meta,
                Column("id", Integer, primary_key=True),
                Column("oid_fk", ForeignKey("other.oid")),
            )
            self.meta.create_all(connection)
            yield
            self.meta.drop_all(connection)

        def test_drop_col_with_fk(self, ops_context, connection, tables):
            ops_context.drop_column("round_trip_table", "oid_fk", self.meta.schema)
            insp = inspect(connection)
            eq_(insp.get_foreign_keys("round_trip_table", schema=self.meta.schema), [])
