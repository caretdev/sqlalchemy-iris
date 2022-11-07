from sqlalchemy.types import TypeDecorator
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import Boolean


ischema = MetaData()


class YESNO(TypeDecorator):
    impl = String

    cache_ok = True

    def __init__(self, length=None, **kwargs):
        super().__init__(length, **kwargs)

    def process_literal_param(self, value, dialect):
        return 'YES' if value else 'NO'

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        return value == 'YES'


schemata = Table(
    "SCHEMATA",
    ischema,
    Column("CATALOG_NAME", String, key="catalog_name"),
    Column("SCHEMA_NAME", String, key="schema_name"),
    Column("SCHEMA_OWNER", String, key="schema_owner"),
    schema="INFORMATION_SCHEMA",
)

tables = Table(
    "TABLES",
    ischema,
    Column("TABLE_CATALOG", String, key="table_catalog"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("TABLE_TYPE", String, key="table_type"),
    schema="INFORMATION_SCHEMA",
)

columns = Table(
    "COLUMNS",
    ischema,
    Column("TABLE_CATALOG", String, key="table_catalog"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    Column("COLUMN_DEFAULT", Integer, key="column_default"),
    Column("IS_NULLABLE", YESNO, key="is_nullable"),
    Column("DATA_TYPE", String, key="data_type"),
    Column(
        "CHARACTER_MAXIMUM_LENGTH", Integer, key="character_maximum_length"
    ),
    Column("NUMERIC_PRECISION", Integer, key="numeric_precision"),
    Column("NUMERIC_SCALE", Integer, key="numeric_scale"),
    Column("COLLATION_NAME", String, key="collation_name"),
    Column("AUTO_INCREMENT", YESNO, key="auto_increment"),
    Column("UNIQUE_COLUMN", YESNO, key="unique_column"),
    Column("PRIMARY_KEY", YESNO, key="primary_key"),
    Column("DESCIPTION", String, key="desciption"),
    schema="INFORMATION_SCHEMA",
)

indexes = Table(
    "INDEXES",
    ischema,
    Column("TABLE_CATALOG", String, key="table_catalog"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("NON_UNIQUE", Boolean, key="non_unique"),
    Column("INDEX_CATALOG", String, key="index_catalog"),
    Column("INDEX_SCHEMA", String, key="index_schema"),
    Column("INDEX_NAME", String, key="index_name"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("ASC_OR_DESC", String, key="asc_or_desc"),
    Column("PRIMARY_KEY", Boolean, key="primary_key"),
    schema="INFORMATION_SCHEMA",
)

key_constraints = Table(
    "KEY_COLUMN_USAGE",
    ischema,
    Column("CONSTRAINT_SCHEMA", String, key="constraint_schema"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    Column("CONSTRAINT_TYPE", String, key="constraint_type"),
    schema="INFORMATION_SCHEMA",
)

constraints = Table(
    "TABLE_CONSTRAINTS",
    ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("CONSTRAINT_TYPE", String, key="constraint_type"),
    schema="INFORMATION_SCHEMA",
)

column_constraints = Table(
    "CONSTRAINT_COLUMN_USAGE",
    ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    schema="INFORMATION_SCHEMA",
)

ref_constraints = Table(
    "REFERENTIAL_CONSTRAINTS",
    ischema,
    Column("CONSTRAINT_CATALOG", String, key="constraint_catalog"),
    Column("CONSTRAINT_SCHEMA", String, key="constraint_schema"),
    Column("CONSTRAINT_TABLE_NAME", String, key="constraint_table_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column(
        "UNIQUE_CONSTRAINT_CATALOG",
        String,
        key="unique_constraint_catalog",
    ),
    Column(
        "UNIQUE_CONSTRAINT_SCHEMA",
        String,
        key="unique_constraint_schema",
    ),
    Column(
        "UNIQUE_CONSTRAINT_TABLE", String, key="unique_constraint_table"
    ),
    Column(
        "UNIQUE_CONSTRAINT_NAME", String, key="unique_constraint_name"
    ),
    Column("MATCH_OPTION", String, key="match_option"),
    Column("UPDATE_RULE", String, key="update_rule"),
    Column("DELETE_RULE", String, key="delete_rule"),
    schema="INFORMATION_SCHEMA",
)

views = Table(
    "VIEWS",
    ischema,
    Column("TABLE_CATALOG", String, key="table_catalog"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("VIEW_DEFINITION", String, key="view_definition"),
    Column("CHECK_OPTION", String, key="check_option"),
    Column("IS_UPDATABLE", String, key="is_updatable"),
    schema="INFORMATION_SCHEMA",
)
