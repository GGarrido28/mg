queries = {
    "postgresql": {
        "get_source_table_schema": "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'",
    },
    "sql_server": {
        "get_source_table_schema": "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'",
    },
}
