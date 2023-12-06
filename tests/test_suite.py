from enum import Enum

from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite import CTETest as _CTETest
from sqlalchemy.testing.suite import DifficultParametersTest as _DifficultParametersTest
from sqlalchemy.testing.suite import (
    BizarroCharacterFKResolutionTest as _BizarroCharacterFKResolutionTest,
)
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing import config
from sqlalchemy.orm import Session
from sqlalchemy import testing
from sqlalchemy import Table, Column, select
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import VARBINARY
from sqlalchemy.types import BINARY
from sqlalchemy_iris import TINYINT
from sqlalchemy.exc import DatabaseError
import pytest

from sqlalchemy.testing.suite import *  # noqa


class CompoundSelectTest(_CompoundSelectTest):
    @pytest.mark.skip()
    def test_limit_offset_aliased_selectable_in_unions(self):
        return


class CTETest(_CTETest):
    @pytest.mark.skip()
    def test_select_recursive_round_trip(self):
        pass


@pytest.mark.skip()
class DifficultParametersTest(_DifficultParametersTest):
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
            expected = assert_data[offset : offset + limit]
            self._assert_result(
                connection,
                select(table).limit(limit).offset(offset),
                expected,
            )


class TinyintTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_tinyint",
            metadata,
            Column("val_tinyint", TINYINT),
        )

    @testing.fixture
    def local_connection(self):
        with testing.db.connect() as conn:
            yield conn

    def test_commits(self, local_connection):
        test_tinyint = self.tables.test_tinyint
        connection = local_connection

        transaction = connection.begin()
        connection.execute(test_tinyint.insert(), dict(val_tinyint=100))
        with pytest.raises(DatabaseError):
            connection.execute(test_tinyint.insert(), dict(val_tinyint=129))
        with pytest.raises(DatabaseError):
            connection.execute(test_tinyint.insert(), dict(val_tinyint=-129))
        transaction.commit()

        result = connection.exec_driver_sql("select * from test_tinyint")
        assert len(result.fetchall()) == 1
        connection.close()


class TransactionTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(20)),
            test_needs_acid=True,
        )

    @testing.fixture
    def local_connection(self):
        with testing.db.connect() as conn:
            yield conn

    def test_commits(self, local_connection):
        users = self.tables.users
        connection = local_connection
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        transaction.commit()

        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()

        transaction = connection.begin()
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 3
        transaction.commit()
        connection.close()

    def test_rollback(self, local_connection):
        """test a basic rollback"""

        users = self.tables.users
        connection = local_connection
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.rollback()
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 0


class IRISExistsTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(20)),
            test_needs_acid=True,
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.users.insert(),
            [
                {"user_id": 1, "user_name": "admin"},
            ],
        )

    def test_exists(self):
        with config.db.connect() as conn:
            with Session(conn) as s:
                assert s.query(
                    select(self.tables.users)
                    .where(self.tables.users.c.user_name == "admin")
                    .exists()
                ).scalar()

                assert not s.query(
                    select(self.tables.users)
                    .where(self.tables.users.c.user_name == "nope")
                    .exists()
                ).scalar()


class IRISBinaryTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("bin1", BINARY(50)),
            Column("bin2", VARBINARY(50)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.data.insert(),
            [
                {"bin1": b"test", "bin2": b"test"},
            ],
        )

    def _assert_result(self, select, result):
        with config.db.connect() as conn:
            eq_(conn.execute(select).fetchall(), result)

    def test_expect_bytes(self):
        self._assert_result(
            select(self.tables.data),
            [(b"test", b"test")],
        )


class SomeType(str, Enum):
    FIRST = "first value"
    SECOND = "second value"


class IRISEnumTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("val", String(50)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.data.insert(),
            [
                {"val": SomeType.FIRST},
                {"val": SomeType.SECOND},
                {"val": None},
            ],
        )

    def _assert_result(self, select, result):
        with config.db.connect() as conn:
            eq_(conn.execute(select).fetchall(), result)

    def test_expect_bytes(self):
        self._assert_result(
            select(self.tables.data),
            [(SomeType.FIRST,), (SomeType.SECOND,), (None,)],
        )


class BizarroCharacterFKResolutionTest(_BizarroCharacterFKResolutionTest):
    @testing.combinations(
        ("id",), ("(3)",), ("col%p",), ("[brack]",), argnames="columnname"
    )
    @testing.variation("use_composite", [True, False])
    @testing.combinations(
        ("plain",),
        # ("(2)",), not in IRIS
        ("per % cent",),
        ("[brackets]",),
        argnames="tablename",
    )
    def test_fk_ref(self, connection, metadata, use_composite, tablename, columnname):
        super().test_fk_ref(connection, metadata, use_composite, tablename, columnname)
