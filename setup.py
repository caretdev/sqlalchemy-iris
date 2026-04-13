from setuptools import setup

setup(
    install_requires=[
        "SQLAlchemy>=1.3",
        "intersystems-irispython~=5.3.2",
    ],
    entry_points={
        "sqlalchemy.dialects": [
            # "iris = sqlalchemy_iris.iris:IRISDialect_iris",
            # "iris.emb = sqlalchemy_iris.embedded:IRISDialect_emb",
            # "iris.irisasync = sqlalchemy_iris.irisasync:IRISDialect_irisasync",
            "iris = sqlalchemy_iris.intersystems:IRISDialect_intersystems",
            "iris.intersystems = sqlalchemy_iris.intersystems:IRISDialect_intersystems",
        ]
    },
)
