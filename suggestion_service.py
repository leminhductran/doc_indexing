from openai import OpenAI
import json
import database as d
import os


client = OpenAI(api_key=os.getenv("API_KEY"))


def get_suggestion(tenant_id: int, document_name: str):

    # Retrieve data feature from database
    queries = d.count_queries(tenant_id, document_name)
    query_id_times_queried = [dict(zip(queries.column_names, row)) for row in queries.result_rows]

    # Prepare data to be processed and generate question
    column_times_queried = column_times_queried_grouping(query_id_times_queried)
    question = generate_question(column_times_queried, document_name)

    return ask(question)


def ask(question: str):
    response = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": """ 
                I need suggestions for indexing columns in the database. 
                I'm using PostgreSQL and indexing using B-tree. 
                You must return for me in JSON with a key starting with 'index_' and a serial number starting at 0. 
                For example, the answer should be like {"index_1": ["col1", "col2"], "index_2": ["col3"]}. 
                The col1 and col2 are the names of the columns that will be indexed. 
                Only giving suggestions for indexed columns improves query performance if no column will is index return empty array. 
                Must return JSON format and do not explain.
            """
        },
        {
            "role": "user",
            "content": question,
        }],
        model="gpt-4o-mini",
    )
    reply_json = response.choices[0].message.content

    return json.loads(reply_json)


def generate_question(columns_queried_times: list, document_name: str):
    prompt = f"The table {document_name} has "

    for columns, times in columns_queried_times.items():
        prompt += f"column {columns} queried {times} times, "

    return prompt + "which column should be indexed?"


def column_times_queried_grouping(query_ids: list[dict]):
    column_times_queried = {}
    for el in query_ids:
        column = el["column_name"]
        if column not in column_times_queried:
            column_times_queried[column] = el["times"]
        else:
            column_times_queried[column] += el["times"]

    return column_times_queried
