import streamlit as st

from awesome_api.utils.postgres_utils import PostgresDataSource

# Streamlit UI
st.title("SQL Query Executor")
st.write("Use this interface to run SQL queries against the database.")

# Input field for SQL query
query = st.text_area("Enter your SQL query here:")

# Button to execute the query
if st.button("Execute Query"):
    if query.strip():
        db = PostgresDataSource()
        df = db.run_select_query(query=query)
        st.dataframe(df)
    else:
        st.warning("Please enter a valid SQL query.")
