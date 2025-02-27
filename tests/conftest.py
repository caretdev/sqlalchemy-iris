from sqlalchemy.dialects import registry
import pytest
import os

from sqlalchemy.testing.plugin.plugin_base import pre

from testcontainers.iris import IRISContainer

registry.register("iris.iris", "sqlalchemy_iris.iris", "IRISDialect_iris")
registry.register("iris.emb", "sqlalchemy_iris.embedded", "IRISDialect_emb")
registry.register(
    "iris.irisasync", "sqlalchemy_iris.irisasync", "IRISDialect_irisasync"
)
registry.register(
    "iris.intersystems", "sqlalchemy_iris.intersystems", "IRISDialect_intersystems"
)

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa

original_pytest_addoption = pytest_addoption # noqa


def pytest_addoption(parser):
    original_pytest_addoption(parser)

    group = parser.getgroup("iris")

    group.addoption(
        "--container",
        action="store",
        default=None,
        type=str,
        help="Docker image with IRIS",
    )

    group.addoption(
        "--driver",
        action="store",
        default=None,
        type=str,
        help="Driver",
    )


@pre
def start_container(opt, file_config):
    global iris
    iris = None
    if not opt.container:
        return

    print("Starting IRIS container:", opt.container)
    iris = IRISContainer(
        opt.container,
        username="sqlalchemy",
        password="sqlalchemy",
        namespace="TEST",
        license_key=(
            os.path.expanduser("~/iris.key")
            if "community" not in opt.container
            else None
        ),
    )
    iris.start()
    dburi = iris.get_connection_url()
    if opt.driver:
        dburi = dburi.replace("iris://", f"iris+{opt.driver}://")
    print("dburi:", dburi)
    opt.dburi = [dburi]


def pytest_unconfigure(config):
    global iris
    if iris and iris._container:
        print("Stopping IRIS container", iris)
        iris.stop()
