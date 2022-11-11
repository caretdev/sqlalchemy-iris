from sqlalchemy.testing.provision import temp_table_keyword_args


@temp_table_keyword_args.for_db("iris")
def _iris_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["GLOBAL TEMPORARY"]}
