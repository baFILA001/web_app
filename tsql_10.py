import streamlit as st
import pandas as pd
import os
import json
from google.cloud import bigquery
from vertexai.preview.language_models import TextGenerationModel

# Set Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/vikash/Desktop/shared-vikas-6b59a69f8888.json'

# Initialize BigQuery client
client = bigquery.Client()

# Initialize TextGenerationModel
generation_model = TextGenerationModel.from_pretrained("text-bison@001")

def sanitize_column_names(df):
    # Sanitize column names to comply with BigQuery requirements
    sanitized_column_names = {}
    for col in df.columns:
        # Replace invalid characters with underscores
        sanitized_name = ''.join([char if char.isalnum() or char == '_' else '_' for char in col])
        # Ensure the name starts with a letter or underscore
        if not sanitized_name[0].isalpha() and not sanitized_name[0] == '_':
            sanitized_name = f'_{sanitized_name}'
        sanitized_column_names[col] = sanitized_name
    # Rename DataFrame columns
    df.rename(columns=sanitized_column_names, inplace=True)
    return df

def create_table_from_df(df, table_name):
    # Sanitize column names before creating the table
    df = sanitize_column_names(df)
    
    try:
        # Define the table reference
        project_id = 'shared-vikas'  # Replace with your project ID
        dataset_id = 'tsql'  # Replace with your dataset ID
        table_ref = client.dataset(dataset_id).table(table_name)
        
        # Load data into BigQuery
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()  # Wait for the job to complete
        
        st.success(f"Table {table_name} created successfully!")
    except Exception as e:
        st.error(f"Error creating table {table_name}: {e}")


# Function to retrieve data from BigQuery table
def get_data(table):
    try:
        # Ensure the table name is fully qualified
        fully_qualified_table = f"shared-vikas.tsql.{table}"  # Replace with your project_id and dataset_id
        query = f"SELECT * FROM `{fully_qualified_table}` LIMIT 5"
        df = client.query(query).result().to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error fetching data from table {table}: {e}")
        return None

def get_schema(table):
    try:
        # Ensure the table name is fully qualified
        fully_qualified_table = f"shared-vikas.tsql.{table}"  # Replace with your project_id and dataset_id
        # Get the table metadata
        table_ref = client.get_table(fully_qualified_table)
        # Retrieve the schema from the table metadata
        schema = table_ref.schema
        # Create a list of tuples to hold schema information (name and type of each field)
        schema_info = [(field.name, field.field_type) for field in schema]
        return schema_info
    except Exception as e:
        st.error(f"Error fetching schema from table {table}: {e}")
        return None

# Function to generate proposed SQL query
def get_proposed_query(table_name, question):
    # Get schema and data for the selected table
    schema_info = get_schema(table_name)
    data = get_data(table_name)
    
    if schema_info is None or data is None:
        st.error("Could not fetch schema or data for the specified table.")
        return None
    
    # Convert data to a string format for the prompt
    data_string = data.to_string(index=False)

    # Prepare the prompt
    prompt = f"""
        Given the following table schema and data:

        Schema for table: {table_name}:
        {schema_info}

        Data for table: {table_name}:
        {data_string}

        User Question: {question}

        Based on the above information, construct a BigQuery SQL query to answer the user question.
    """
    
    # Get response from the model
    try:
        response = generation_model.predict(prompt)
        # Return the generated query text
        proposed_query = response.text.strip()
        return proposed_query
    except Exception as e:
        st.error(f"Error generating proposed query: {e}")
        return None

# Define Streamlit app
# Define Streamlit app
def app():
    st.title("SQLify")

    # File uploader widget
    uploaded_file = st.file_uploader("Upload a CSV file", type="csv")
    
    if uploaded_file is not None:
        # Read CSV file into DataFrame
        df = pd.read_csv(uploaded_file)
        st.write("Preview of uploaded data:")
        st.dataframe(df)
        
        # Allow user to specify table name
        table_name = st.text_input("Enter table name to create in BigQuery:")
        
        if st.button("Create Table"):
            # Create BigQuery table from DataFrame
            create_table_from_df(df, table_name)
    
    # Retrieve all tables from the dataset
    dataset_id = 'tsql'  # Replace with your dataset ID
    dataset_ref = client.dataset(dataset_id)
    tables_list = [table.table_id for table in client.list_tables(dataset_ref)]
    
    # Dropdown menu for table selection
    selected_table = st.selectbox("Select Table:", tables_list)
    
    # User input
    user_question = st.text_input("Enter your question:")
    
    if user_question:
        # Get proposed query for the selected table
        proposed_query = get_proposed_query(selected_table, user_question)
        if proposed_query is not None:
            st.write("\nProposed Query: \n", proposed_query)

            # Execute the proposed query and get results
            try:
                query_result = client.query(proposed_query).result().to_dataframe()
                st.write("\nQuery Result:\n", query_result)

                # Provide a summary of the query result
                if not query_result.empty:
                    summary = f"The dataset contains {len(query_result)} rows."
                    numerical_cols = [col for col in query_result.columns if query_result[col].dtype in ['int64', 'float64']]
                    for col in numerical_cols:
                        summary += f"\nThe average value in '{col}' column is {query_result[col].mean():.2f}."
                        summary += f"\nThe minimum value in '{col}' column is {query_result[col].min()}."
                        summary += f"\nThe maximum value in '{col}' column is {query_result[col].max()}."
                    st.write("\nSummary:", summary)
                else:
                    st.write("\nNo results found.")
            except Exception as e:
                st.error(f"Error executing query: {e}")

if __name__ == "__main__":
    app()

