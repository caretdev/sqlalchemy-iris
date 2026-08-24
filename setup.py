from setuptools import setup

setup(
    install_requires=[
        "SQLAlchemy>=1.3",
        "intersystems-irispython~=5.3.2",
        "iris-embedded-python-wrapper>=0.5.23",
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "iris = sqlalchemy_iris.intersystems:IRISDialect_intersystems",
            "iris.community = sqlalchemy_iris.iris:IRISDialect_iris",
            "iris.emb = sqlalchemy_iris.embedded:IRISDialect_emb",
            "iris.intersystems = sqlalchemy_iris.intersystems:IRISDialect_intersystems",
        ]
    },
)
