from enum import Enum

from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest
from sqlalchemy.testing.suite import FutureTableDDLTest as _FutureTableDDLTest
from sqlalchemy.testing.suite import CTETest as _CTETest
from sqlalchemy.testing.suite import DifficultParametersTest as _DifficultParametersTest
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing import config
from sqlalchemy.orm import Session
from sqlalchemy import testing
from sqlalchemy import Table, Column, select, func, text
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import VARBINARY
from sqlalchemy.types import TEXT
from sqlalchemy.types import BINARY
from sqlalchemy_iris import TINYINT
from sqlalchemy_iris import INTEGER
from sqlalchemy_iris import IRISListBuild
from sqlalchemy_iris import IRISVector
from sqlalchemy.exc import DatabaseError

import pytest

from sqlalchemy.testing.suite import *  # noqa

from sqlalchemy import __version__ as sqlalchemy_version

if sqlalchemy_version.startswith("2."):
    from sqlalchemy.testing.suite import (
        BizarroCharacterFKResolutionTest as _BizarroCharacterFKResolutionTest,
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
        def test_fk_ref(
            self, connection, metadata, use_composite, tablename, columnname
        ):
            super().test_fk_ref(
                connection, metadata, use_composite, tablename, columnname
            )


class CompoundSelectTest(_CompoundSelectTest):
    @pytest.mark.skip()
    def test_limit_offset_aliased_selectable_in_unions(self):
        return


class CTETest(_CTETest):
    @pytest.mark.skip()
    def test_select_recursive_round_trip(self):
        pass


class DifficultParametersTest(_DifficultParametersTest):

    tough_parameters = _DifficultParametersTest.tough_parameters

    @tough_parameters
    def test_round_trip_same_named_column(self, paramname, connection, metadata):
        if paramname == "dot.s":
            # not supported
            pytest.skip()
            return
        super().test_round_trip_same_named_column(paramname, connection, metadata)


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


class IRISListBuildTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("val", IRISListBuild(10, float)),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            data=(
                ("val",),
                ([1.0] * 50,),
                ([1.23] * 50,),
                ([i for i in range(0, 50)],),
                (None,),
            )
        )

    def _assert_result(self, select, result):
        with config.db.connect() as conn:
            eq_(conn.execute(select).fetchall(), result)

    def test_listbuild(self):
        self._assert_result(
            select(self.tables.data),
            [
                ([1.0] * 50,),
                ([1.23] * 50,),
                ([i for i in range(0, 50)],),
                (None,),
            ],
        )
        self._assert_result(
            select(self.tables.data).where(self.tables.data.c.val == [1.0] * 50),
            [
                ([1.0] * 50,),
            ],
        )

        self._assert_result(
            select(
                self.tables.data,
                self.tables.data.c.val.func("$listsame", [1.0] * 50).label("same"),
            ).limit(1),
            [
                ([1.0] * 50, 1),
            ],
        )


class IRISVectorTest(fixtures.TablesTest):
    __backend__ = True

    __requires__ = ("iris_vector",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("id", INTEGER),
            Column("emb", IRISVector(3, float)),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            data=(
                (
                    "id",
                    "emb",
                ),
                (
                    1,
                    [1, 1, 1],
                ),
                (
                    2,
                    [2, 2, 2],
                ),
                (
                    3,
                    [1, 1, 2],
                ),
            )
        )

    def _assert_result(self, select, result):
        with config.db.connect() as conn:
            eq_(conn.execute(select).fetchall(), result)

    def test_vector(self):
        self._assert_result(
            select(self.tables.data.c.emb),
            [
                ([1, 1, 1],),
                ([2, 2, 2],),
                ([1, 1, 2],),
            ],
        )
        self._assert_result(
            select(self.tables.data.c.id).where(self.tables.data.c.emb == [2, 2, 2]),
            [
                (2,),
            ],
        )

    def test_cosine(self):
        self._assert_result(
            select(
                self.tables.data.c.id,
            ).order_by(self.tables.data.c.emb.cosine([1, 1, 1])),
            [
                (1,),
                (2,),
                (3,),
            ],
        )

    def test_cosine_distance(self):
        self._assert_result(
            select(
                self.tables.data.c.id,
            ).order_by(1 - self.tables.data.c.emb.cosine_distance([1, 1, 1])),
            [
                (1,),
                (2,),
                (3,),
            ],
        )

    def test_max_inner_product(self):
        self._assert_result(
            select(
                self.tables.data.c.id,
            ).order_by(self.tables.data.c.emb.max_inner_product([1, 1, 1])),
            [
                (1,),
                (3,),
                (2,),
            ],
        )


class ConcatTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("sometext", TEXT),
            Column("testdata", TEXT),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            data=(
                (
                    "sometext",
                    "testdata",
                ),
                (
                    "sometestdata",
                    "test",
                ),
            )
        )

    def _assert_result(self, select, result):
        with config.db.connect() as conn:
            eq_(conn.execute(select).fetchall(), result)

    def test_concat_func(self):
        self._assert_result(
            select(
                self.tables.data.c.sometext,
            ).filter(
                self.tables.data.c.sometext
                == func.concat("some", self.tables.data.c.testdata, "data")
            ),
            [
                ("sometestdata",),
            ],
        )


class TableDDLTest(_TableDDLTest):
    # IRIS does not want to update comments on the fly
    def test_add_table_comment(self, connection):
        pass

    def test_drop_table_comment(self, connection):
        pass


class FutureTableDDLTest(_FutureTableDDLTest):
    # IRIS does not want to update comments on the fly
    def test_add_table_comment(self, connection):
        pass

    def test_drop_table_comment(self, connection):
        pass


class IRISPaginationTest(fixtures.TablesTest):

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", String(50)),
        )
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("username", String(30)),
            Column("email", String(100)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.data.insert(),
            [{"id": i, "value": f"value_{i}"} for i in range(1, 21)],
        )
        connection.execute(
            cls.tables.users.insert(),
            [
                {
                    "user_id": i,
                    "username": f"user_{i}",
                    "email": f"user_{i}@example.com",
                }
                for i in range(1, 31)
            ],
        )

    def test_pagination_single_table(self):
        """Test basic pagination on single table"""
        with config.db.connect() as conn:

            # Test first page
            result = conn.execute(
                select(self.tables.data).limit(10).offset(0)
            ).fetchall()

            assert len(result) == 10
            assert result[0].value == "value_1"
            assert result[9].value == "value_10"

            # Test second page
            result = conn.execute(
                select(self.tables.data).limit(10).offset(10)
            ).fetchall()
            assert len(result) == 10
            assert result[0].value == "value_11"
            assert result[9].value == "value_20"

    def test_pagination_with_order(self):
        """Test pagination with explicit ordering"""
        with config.db.connect() as conn:
            # Test ordered pagination on users table by user_id (numeric order)
            result = conn.execute(
                select(self.tables.users)
                .order_by(self.tables.users.c.user_id)
                .limit(5)
                .offset(0)
            ).fetchall()
            assert len(result) == 5
            assert result[0].username == "user_1"
            assert result[4].username == "user_5"

            # Test second page with ordering
            result = conn.execute(
                select(self.tables.users)
                .order_by(self.tables.users.c.user_id)
                .limit(5)
                .offset(5)
            ).fetchall()
            assert len(result) == 5
            assert result[0].username == "user_6"
            assert result[4].username == "user_10"

    def test_pagination_two_tables_join(self):
        """Test pagination with JOIN between two tables"""
        with config.db.connect() as conn:
            # Create a join query with pagination
            # Join where data.id matches user.user_id for first 20 records
            query = (
                select(
                    self.tables.data.c.value,
                    self.tables.users.c.username,
                    self.tables.users.c.email,
                )
                .select_from(
                    self.tables.data.join(
                        self.tables.users,
                        self.tables.data.c.id == self.tables.users.c.user_id,
                    )
                )
                .order_by(self.tables.data.c.id)
                .limit(5)
                .offset(5)
            )

            result = conn.execute(query).fetchall()
            assert len(result) == 5
            assert result[0].value == "value_6"
            assert result[0].username == "user_6"
            assert result[4].value == "value_10"
            assert result[4].username == "user_10"

    def test_pagination_large_offset(self):
        """Test pagination with larger offset values"""
        with config.db.connect() as conn:
            # Test pagination near the end of users table
            result = conn.execute(
                select(self.tables.users)
                .order_by(self.tables.users.c.user_id)
                .limit(5)
                .offset(25)
            ).fetchall()
            assert len(result) == 5
            assert result[0].user_id == 26
            assert result[4].user_id == 30

            # Test offset beyond available data
            result = conn.execute(
                select(self.tables.users)
                .order_by(self.tables.users.c.user_id)
                .limit(10)
                .offset(35)
            ).fetchall()
            assert len(result) == 0

    def test_pagination_count_total(self):
        """Test getting total count for pagination metadata"""
        with config.db.connect() as conn:
            # Get total count of data table
            total_data = conn.execute(
                select(func.count()).select_from(self.tables.data)
            ).scalar()
            assert total_data == 20

            # Get total count of users table
            total_users = conn.execute(
                select(func.count()).select_from(self.tables.users)
            ).scalar()
            assert total_users == 30

            # Verify pagination math
            page_size = 7
            total_pages_data = (total_data + page_size - 1) // page_size
            assert total_pages_data == 3  # 20 records / 7 per page = 3 pages

            # Test last page
            result = conn.execute(
                select(self.tables.data)
                .order_by(self.tables.data.c.id)
                .limit(page_size)
                .offset((total_pages_data - 1) * page_size)
            ).fetchall()
            assert len(result) == 6  # Last page has 6 records (20 - 14)


class Issue20Test(fixtures.TablesTest):

    def test_with(self):
        sql = """
            WITH cte AS (
                SELECT 123 as n, :param as message
            ),
            cte1 AS (
                SELECT 345 as n, :param1 as message
            ),
            cte2 AS (
                SELECT *, :param2 as message2
                FROM cte
            )
            SELECT *, :global as test FROM %s;
        """

        params = {
            "param": "hello",
            "param2": "hello2",
            "param1": "hello1",
            "global": "global_value",
        }
        with config.db.connect() as conn:
            result = conn.execute(
                text(sql % "cte"),
                params,
            ).fetchall()
            assert result == [(123, "hello", "global_value")]

        with config.db.connect() as conn:
            result = conn.execute(
                text(sql % "cte1"),
                params,
            ).fetchall()
            assert result == [(345, "hello1", "global_value")]

        with config.db.connect() as conn:
            result = conn.execute(
                text(sql % "cte2"),
                params,
            ).fetchall()
            assert result == [(123, "hello", "hello2", "global_value")]


@pytest.mark.skip
class ComponentReflectionTest(_ComponentReflectionTest):
    # TODO: fails when run in bulk
    pass
