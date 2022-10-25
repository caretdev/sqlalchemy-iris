from sqlalchemy.testing.suite.test_reflection import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
import pytest

from sqlalchemy.testing.suite import * # noqa


class CompoundSelectTest(_CompoundSelectTest):
    @pytest.mark.skip()
    def test_limit_offset_aliased_selectable_in_unions(self):
        return


@pytest.mark.skip()
class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    pass
