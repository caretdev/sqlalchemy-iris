from sqlalchemy.dialects import registry
import pytest

registry.register("iris.iris", "sqlalchemy_iris.iris", "IRISDialect_iris")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
