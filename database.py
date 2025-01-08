import clickhouse_connect
import os


client = clickhouse_connect.get_client(
    host=os.getenv("HOST"),
    port=os.getenv("PORT"),
    username=os.getenv("USERNAME"), 
    password=os.getenv("PASSWORD"),
    database=os.getenv("DATABASE"))


def insert_query(query: str, tenant_id: int):
    parameters = {'v1': query, 'v2': tenant_id}
    statement = "INSERT INTO sql_query (query, tenant_id) VALUES ({v1:String}, {v2: Int64})"

    return client.command(statement, parameters=parameters)


def select_query(query: str, tenant_id: int):
    params = {'v1': query, 'v2': tenant_id}
    statement = "SELECT id FROM sql_query WHERE query = %(v1)s AND tenant_id = %(v2)s"
    queries = client.query(statement, parameters=params)
    return [dict(zip(queries.column_names, row)) for row in queries.result_rows]


def insert_query_feature(feature_data: list[list]):
    columns = ['query_id', 'table_name', 'column_name', 'clause', 'tenant_id']

    return client.insert('sql_query_feature', feature_data, column_names=columns)


def insert_query_execution(execution_data: dict):
    statement = """INSERT INTO sql_query_execution (query_id, executed_at, execution_time, tenant_id) 
    VALUES ({query_id:String}, {executed_at:DateTime64}, {execution_time:Float64}, {tenant_id:Int64})
    """

    return client.command(statement, parameters=execution_data)


def count_queries(tenant_id: int, document_name: str):
    params = {'v1': tenant_id, 'v2': document_name}
    statement = """
    SELECT 
    f.query_id, f.column_name, 
    COUNT(1) AS times 
    FROM 
        sql_query_execution e
    INNER JOIN 
        sql_query_feature f
    ON 
        e.query_id = f.query_id 
    WHERE
        f.tenant_id =  %(v1)s
    AND
        f.table_name = %(v2)s
    GROUP BY 
        f.query_id, f.column_name

    """
    result = client.query(statement, params)

    return result
