import dlt

@dlt.table(
    name="users.bronze.users_data"
)
def load_from_stream():
    df = spark.sql("""
                   SELECT 
                    *
                    FROM STREAM read_files(
                        '/Volumes/users/default/data',
                        format => 'json',
                        schema => 'id int, name string, email string, age int, country string'
                    )
                   """)
    return df 