from sqlalchemy import Column, MetaData, Table, select
from sqlalchemy.sql.sqltypes import Integer, UUID
from sqlalchemy_iris import IRISVector
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
import uuid

DATABASE_URL = "iris://_SYSTEM:SYS@localhost:1972/USER"
engine = create_engine(DATABASE_URL, echo=False)

# Create a table metadata
metadata = MetaData()


def main():
    demo_table = Table(
        "demo_table",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("uuid", UUID),
        Column("embedding", IRISVector(item_type=float, max_items=3)),
    )

    demo_table.drop(engine, checkfirst=True)
    demo_table.create(engine, checkfirst=True)
    with engine.connect() as conn:
        conn.execute(
            demo_table.insert(),
            [
                {"uuid": uuid.uuid4(), "embedding": [1, 2, 3]},
                {"uuid": uuid.uuid4(), "embedding": [2, 3, 4]},
            ],
        )
        conn.commit()
        result = conn.execute(
            demo_table.select()
        ).fetchall()
        print("result", result)


main()
