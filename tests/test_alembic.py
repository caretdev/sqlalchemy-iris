try:
    import alembic  # noqa
except:  # noqa
    pass
else:
    from alembic.testing.suite.test_op import (
        BackendAlterColumnTest as _BackendAlterColumnTest,
    )
    from alembic.testing.suite.test_autogen_diffs import (
        AutoincrementTest as _AutoincrementTest,
    )
    from alembic.testing.suite import *  # noqa

    class BackendAlterColumnTest(_BackendAlterColumnTest):
        def test_rename_column(self):
            # IRIS Uppercases new names
            self._run_alter_col({}, {"name": "NEWNAME"})

    class AutoincrementTest(_AutoincrementTest):
        # pk don't change type
        def test_alter_column_autoincrement_pk_implicit_true(self):
            pass

        def test_alter_column_autoincrement_pk_explicit_true(self):
            pass
