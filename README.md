sqlalchemy-iris
===

An InterSystems IRIS dialect for SQLAlchemy.

Pre-requisites
---

This dialect requires SQLAlchemy, InterSystems DB-API driver. They are specified as requirements so ``pip``
will install them if they are not already in place. To install, just:

    pip install sqlalchemy-iris

Usage
---

In your Python app, you can connect to the database via:

    from sqlalchemy import create_engine
    engine = create_engine("iris://_SYSTEM:SYS@localhost:1972/USER")
