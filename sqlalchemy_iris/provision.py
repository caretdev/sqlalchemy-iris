from sqlalchemy.testing.provision import temp_table_keyword_args, create_db, drop_db


@temp_table_keyword_args.for_db("iris")
def _iris_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["GLOBAL TEMPORARY"]}


@create_db.for_db("iris")
def _iris_create_db(cfg, eng, ident):
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql("create database %s" % ident)


@drop_db.for_db("iris")
def _iris_drop_db(cfg, eng, ident):
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql("drop database %s" % ident)
