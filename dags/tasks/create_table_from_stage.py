import os
import snowflake.connector as sc

def create_table_from_stage(table_name, dataset_file):
    conn_params = {
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'user': os.environ.get('SNOWFLAKE_USER'),
        'role': 'ACCOUNTADMIN',
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'private_key_file': os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA'),
    }

    ctx = sc.connect(**conn_params)
    with ctx.cursor() as cs:
        cs.execute(f"""
            CREATE OR REPLACE TABLE "{os.environ.get('SNOWFLAKE_DATABASE')}"."{os.environ.get('SNOWFLAKE_SCHEMA')}"."{table_name}"
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
            LOCATION=>'@"{os.environ.get('SNOWFLAKE_DATABASE')}"."{os.environ.get('SNOWFLAKE_SCHEMA')}"."{os.environ.get('SNOWFLAKE_PROJECT')}_EXTERNAL_STAGE"/raw/{dataset_file}'
            , FILE_FORMAT=>'"{os.environ.get('SNOWFLAKE_DATABASE')}"."{os.environ.get('SNOWFLAKE_SCHEMA')}"."{os.environ.get('SNOWFLAKE_PROJECT')}_FILE_FORMAT"'
            , IGNORE_CASE=>TRUE
                    )
                )
            );
        """)
        cs.execute(f"""
            COPY INTO "{os.environ.get('SNOWFLAKE_DATABASE')}"."{os.environ.get('SNOWFLAKE_SCHEMA')}"."{table_name}"
            FROM '@"{os.environ.get('SNOWFLAKE_DATABASE')}"."{os.environ.get('SNOWFLAKE_SCHEMA')}"."{os.environ.get('SNOWFLAKE_PROJECT')}_EXTERNAL_STAGE"/raw/{dataset_file}'
            FILE_FORMAT = '"{os.environ.get('SNOWFLAKE_DATABASE')}"."{os.environ.get('SNOWFLAKE_SCHEMA')}"."{os.environ.get('SNOWFLAKE_PROJECT')}_FILE_FORMAT"'
            MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';
        """)