import sys
sys.path.insert(1, '/home/irisowner/sqlalchemy')
sys.path.insert(1, '/home/irisowner/intersystems-irispython')

from sqlalchemy.dialects import registry
import pytest

registry.register("iris.iris", "sqlalchemy_iris.iris", "IRISDialect_iris")
registry.register("iris.emb", "sqlalchemy_iris.embedded", "IRISDialect_emb")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
