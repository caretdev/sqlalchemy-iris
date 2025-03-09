sqlalchemy-iris
===

An InterSystems IRIS dialect for SQLAlchemy.

Pre-requisites
---

This dialect requires SQLAlchemy, InterSystems DB-API driver. They are specified as requirements so ``pip``
will install them if they are not already in place. To install, just:

```shell
pip install sqlalchemy-iris
```

Or to use InterSystems official driver support

```shell
pip install sqlalchemy-iris[intersystems]
```

Usage
---

In your Python app, you can connect to the database via:

```python
from sqlalchemy import create_engine
engine = create_engine("iris://_SYSTEM:SYS@localhost:1972/USER")
```

To use with Python Embedded mode, when run next to IRIS

```python
from sqlalchemy import create_engine
engine = create_engine("iris+emb:///USER")
```

To use with InterSystems official driver, does not work in Python Embedded mode

```python
from sqlalchemy import create_engine
engine = create_engine("iris+intersystems://_SYSTEM:SYS@localhost:1972/USER")
```

IRIS Cloud SQL requires SSLContext

```python
url = engine.URL.create(
    drivername="iris",
    host=host,
    port=443,
    username='SQLAdmin',
    password=password,
    database='USER',
)

sslcontext = ssl.create_default_context(cafile="certificateSQLaaS.pem")

engine = create_engine(url, connect_args={"sslcontext": sslcontext})
```

InterSystems IRIS
---

You can run your instance of InterSystems IRIS Community Edition with Docker

```shell
docker run -d --name iris \
 -p 1972:1972 \
 -p 52773:52773 \
 -e IRIS_USERNAME=_SYSTEM \
 -e IRIS_PASSWORD=SYS \
 intersystemsdc/iris-community:preview
```

Examples
===

IRISVector
---

```python
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


class Base(DeclarativeBase):
    pass


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
```

_Port 1972 is used for binary communication (this driver, xDBC and so on), and 52773 is for web (Management Portal, IRIS based web-applications and API's)._

The System Management Portal is available by URL: `http://localhost:52773/csp/sys/UtilHome.csp`
