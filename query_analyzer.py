from sql_metadata import Parser
import database as d


def analyze(msg):
    print("analyzing...")
    parser = Parser(msg["data"]["query"])

    # Check for "unsuitable" queries: no tables, 'join' clause in query
    suitability = check_query_corner_cases(parser)
    if suitability == False:
        return
    
    # Getting elements from the data
    tenant_id = int(msg.get("tenant_id"))
    columns_dict = parser.columns_dict # Returns columns after specific clauses
    table_name = parser.tables[0]
    execution_time = float(msg["data"]["exe_time_ms"])
    executed_at = float(msg.get("time"))

    # Generalizing to add to sql_query table
    generalized_query = parser.generalize
    queries = d.select_query(generalized_query, tenant_id)

    if not queries:
        d.insert_query(generalized_query, tenant_id)

        # Re-selecting to get the query_id
        queries = d.select_query(generalized_query, tenant_id)
        query_id = str(queries[0].get('id'))

        # Adding the relevant columns to the features table
        feature_data = []
        for clause in columns_dict:
            if clause in ['where', 'order by', 'group by']:
                for column in columns_dict[clause]:
                    feature = [query_id, table_name, column, clause, tenant_id]
                    feature_data.append(feature)

        d.insert_query_feature(feature_data)
        
    execution_data = {
        "query_id": str(queries[0].get('id')), 
        "executed_at": executed_at,
        "execution_time": execution_time,
        "tenant_id": tenant_id
    }

    d.insert_query_execution(execution_data)


def check_query_corner_cases(parser: Parser):
    if parser.tables == 0 or 'join' in parser.columns_dict:
        return False
    return True
