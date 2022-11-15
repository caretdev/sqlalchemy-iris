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

InterSystems IRIS
---

You can run your instance of InterSystems IRIS Community Edition with Docker 

```shell
docker run -d --name iris \
 -p 1972:1972 \
 -p 52773:52773 \
 intersystemsdc/iris-community:preview
```

_Port 1972 is used for binary communication (this driver, xDBC and so on), and 52773 is for web (Management Portal, IRIS based web-applications and API's)._

The System Management Portal is available by URL: `http://localhost:52773/csp/sys/UtilHome.csp`

The default password - `SYS`, has to be changed after the first login to the management portal. Or start the container with a command that resets the change password flag

```shell
docker run -d --name iris \
 -p 1972:1972 \
 -p 52773:52773 \
 intersystemsdc/iris-community:preview \
 -a "iris session iris -U%SYS '##class(Security.Users).UnExpireUserPasswords(\"*\")'"
```
