from setuptools import setup

setup(
    install_requires=[
        "SQLAlchemy>=1.3"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "iris = sqlalchemy_iris.iris:IRISDialect_iris",
            "iris.emb = sqlalchemy_iris.embedded:IRISDialect_emb",
            "iris.irisasync = sqlalchemy_iris.irisasync:IRISDialect_irisasync",
        ]
    },
)
