import streamlit as st
import pandas as pd
import os
from google.cloud import bigquery
from vertexai.preview.language_models import TextGenerationModel
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"])
# Initialize BigQuery client
client = bigquery.Client()

# Initialize TextGenerationModel
generation_model = TextGenerationModel.from_pretrained("text-bison@001")

# Function to sanitize column names
def sanitize_column_names(df):
    sanitized_column_names = {}
    for col in df.columns:
        sanitized_name = ''.join([char if char.isalnum() or char == '_' else '_' for char in col])
        if not sanitized_name[0].isalpha() and not sanitized_name[0] == '_':
            sanitized_name = f'_{sanitized_name}'
        sanitized_column_names[col] = sanitized_name
    df.rename(columns=sanitized_column_names, inplace=True)
    return df

# Function to create table from DataFrame
def create_table_from_df(df, dataset_id, table_name):
    df = sanitize_column_names(df)
    try:
        table_ref = client.dataset(dataset_id).table(table_name)
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()
        st.success(f"Table {table_name} created successfully!")
    except Exception as e:
        st.error(f"Error creating table {table_name}: {e}")

# Function to generate proposed SQL query
def get_proposed_query(table_name, question):
    schemas = get_schemas([table_name])
    data = get_data([table_name])
    
    # Parameters for text generation
    parameters = {
        "temperature": 0.2,
        "max_output_tokens": 1024
    }

    # Improved prompt with examples and additional instructions
    prompt = f"""
        # Context
        Given the following table schemas and data:

        Schema for table: {table_name}:
        {schemas}

        Data for table: {table_name}:
        {data}

        # Examples of user questions and corresponding SQL queries
        Examples:
        1. User Question: "How many transactions occurred on 29th June 2017?"
           SQL Query: SELECT COUNT(*) FROM {table_name} WHERE DATE = '29-Jun-17';

        # Your task
        As a senior analyst, write a BigQuery SQL query to answer the following user question:

        {question}

        # Guidelines
        When constructing SQL statements, follow these rules:
        - Use appropriate column names and data types as per the provided schema.
        - Use proper date formats and operations when working with date fields.
        - Ensure the query adheres to the table structure and available data.
    """

    # Use the TextGenerationModel to predict the proposed query
    response = generation_model.predict(
        prompt=prompt,
        **parameters
    )

    # Process the response and clean up the generated query
    proposed_query = response.text.replace("```sql", "").replace("```", "").replace("[", "`").replace("]", "`")

    return proposed_query

# Function to retrieve schemas from BigQuery tables
def get_schemas(tables):
    schemas = ""
    for table in tables:
        table_id = table.split(".")[-1]  # Get table name from table ID
        querystring = f"""
        SELECT
        column_name,
        data_type
        FROM
        `{table.split('.')[0]}`.INFORMATION_SCHEMA.COLUMNS
        WHERE
        table_name = "{table_id}";
        """
        schemas += f"\n\nSchema for table: {table}:\n\n"
        schemas += str(client.query(querystring).result().to_dataframe())
    return schemas

# Function to retrieve data from BigQuery tables
def get_data(tables):
    data = ""
    for table in tables:
        querystring = f"""
        SELECT *
        FROM
        `{table}`
        LIMIT 5
        """
        data += f"\n\nData for table: {table}:\n\n"
        data += str(client.query(querystring).result().to_dataframe())
    return data

# Define Streamlit app
def app():
    st.title("CSV to SQL")

    # Ask user to upload CSV file
    st.write("Please upload the CSV file:")
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

    if uploaded_file is not None:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(uploaded_file)

        # Ask user for dataset and table names
        dataset_name = st.text_input("Enter the dataset name:")
        table_name = st.text_input("Enter the table name:")

        if dataset_name != "" and table_name != "":
            # Create BigQuery table from DataFrame
            dataset_id = 'tsql'  # Replace with your dataset ID
            create_table_from_df(df, dataset_id, table_name)

            # User input
            user_question = st.text_input("Enter your question:")
            
            if user_question:
                # Get proposed query for the selected table
                table_id = f"{dataset_id}.{table_name}"
                proposed_query = get_proposed_query(table_id, user_question)
                st.write("\nProposed Query: \n", proposed_query)

                # Execute proposed query and get results
                query_result = client.query(proposed_query).result().to_dataframe()
                st.write("\nQuery Result:\n", query_result)

                # Summarization of the query result
                # Function for summarization is not provided, please implement based on your requirements

if __name__ == "__main__":
    app()
