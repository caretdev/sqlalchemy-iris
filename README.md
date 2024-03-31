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

Usage
---

In your Python app, you can connect to the database via:

```python
from sqlalchemy import create_engine
engine = create_engine("iris://_SYSTEM:SYS@localhost:1972/USER")
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

_Port 1972 is used for binary communication (this driver, xDBC and so on), and 52773 is for web (Management Portal, IRIS based web-applications and API's)._

The System Management Portal is available by URL: `http://localhost:52773/csp/sys/UtilHome.csp`
