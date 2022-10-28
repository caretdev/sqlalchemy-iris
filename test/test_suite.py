from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing import fixtures, AssertsExecutionResults, AssertsCompiledSQL
from sqlalchemy import testing
from sqlalchemy import Table, Column, Integer, String, select
import pytest

from sqlalchemy.testing.suite import *  # noqa


class CompoundSelectTest(_CompoundSelectTest):
    @pytest.mark.skip()
    def test_limit_offset_aliased_selectable_in_unions(self):
        return


@pytest.mark.skip()
class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    pass


class FetchLimitOffsetTest(_FetchLimitOffsetTest):

    def test_simple_offset_no_order(self, connection):
        table = self.tables.some_table
        self._assert_result(
            connection,
            select(table).offset(2),
            [(3, 3, 4), (4, 4, 5), (5, 4, 6)],
        )
        self._assert_result(
            connection,
            select(table).offset(3),
            [(4, 4, 5), (5, 4, 6)],
        )

    @testing.combinations(
        ([(2, 0), (2, 1), (3, 2)]),
        ([(2, 1), (2, 0), (3, 2)]),
        ([(3, 1), (2, 1), (3, 1)]),
        argnames="cases",
    )
    def test_simple_limit_offset_no_order(self, connection, cases):
        table = self.tables.some_table
        connection = connection.execution_options(compiled_cache={})

        assert_data = [(1, 1, 2), (2, 2, 3), (3, 3, 4), (4, 4, 5), (5, 4, 6)]

        for limit, offset in cases:
            expected = assert_data[offset: offset + limit]
            self._assert_result(
                connection,
                select(table).limit(limit).offset(offset),
                expected,
            )


class MiscTest(AssertsExecutionResults, AssertsCompiledSQL, fixtures.TablesTest):

    __backend__ = True

    __only_on__ = "iris"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("y", Integer),
            Column("z", String(50)),
        )

    # def test_compile(self):
    #     table = self.tables.some_table

    #     stmt = select(table.c.id, table.c.x).offset(20).limit(10)

