import openai
import re
import streamlit as st
import yaml
import snowflake.connector
import requests
import logging
import pandas as pd


schemas = ['ABC.RAW']

_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "XXXXXXX"
}

class snowflakeDB(object):
    def __init__(self, user, password):
        self.user = user
        self.password = password

    def get_cursor(self, role = 'xxxxx'):
        logging.info('Getting Snoflake SSO Token')
        request_params = {"grant_type": "password", "username": self.user, "password": self.user}
        requests.post('xxxxx', data=request_params, headers=_HEADERS)

        ctx = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account='xxxxx',
            authenticator='xxxxx',
            paramstyle='qmark',
            database='xxxxx',
            schema='FACT',
            warehouse='xxxxx',
            role=role,
            )

        cur = ctx.cursor()  
         
        return cur



# streamlit styling

# Layout
st.set_page_config(page_title="💻 Snow Bot", page_icon="❄️", layout="centered")

# Header
st.markdown("""
    # 💻 Snow Bot
    Welcome to Snow Bot! You can use this app to interact with our database. Just type your questions in the chat box below.
    """,
    unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown(
        '<h6>Made in &nbsp<img src="https://streamlit.io/images/brand/streamlit-mark-color.png" alt="Streamlit logo" height="16">&nbsp </h6>',
        unsafe_allow_html=True,
    )

# Initialize the chat messages history
openai.api_key = st.secrets.OPENAI_API_KEY

# Function to get table context
@st.cache_data(show_spinner=False)
def get_table_context(table_name: str, metadata_query: str = None):
    table = table_name.split(".")
    cur = snowflakeDB('xxxxx', 'xxxxx').get_cursor()
    cur.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE FROM {table[0].upper()}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{table[1].upper()}' AND TABLE_NAME = '{table[2].upper()}'
        """,
    )
    rows = cur.fetchall()

    col_df = pd.DataFrame(rows, columns=['COLUMN_NAME','DATA_TYPE'])
    columns = "\n".join(
        [
            f"- **{col_df['COLUMN_NAME'][i]}**: {col_df['DATA_TYPE'][i]}"
            for i in range(len(col_df["COLUMN_NAME"]))
        ]
    )
    # Query to get table description
    table_description_query = f"SELECT TABLE_NAME, COMMENT FROM {table[0].upper()}.information_schema.tables WHERE TABLE_CATALOG = '{table[0].upper()}' AND TABLE_SCHEMA = '{table[1].upper()}' AND TABLE_NAME = '{table[2].upper()}';"
    cur.execute(table_description_query)
    rows = cur.fetchall()
    table_description_result_df = pd.DataFrame(rows, columns=['TABLE_NAME','COMMENT'])
    table_description = table_description_result_df["COMMENT"][0]

    context = f"""
Here is the table name <tableName> {'.'.join(table)} </tableName>

<tableDescription>{table_description}</tableDescription>

Here are the columns of the {'.'.join(table)}

<columns>\n\n{columns}\n\n</columns>
    """
    if metadata_query:
        cur.execute(metadata_query)
        rows = cur.fetchall()
        metadata = pd.DataFrame(rows, columns=['COLUMN_NAME','COMMENT'])
        metadata = "\n".join(
            [
                f"- **{metadata['COLUMN_NAME'][i]}**: {metadata['COMMENT'][i]}"
                for i in range(len(metadata["COLUMN_NAME"]))
            ]
        )
        context = context + f"\n\nAvailable variables by COLUMN_NAME:\n\n{metadata}"
    return context

# Function to get system prompt
def get_system_prompt(table_context):
    GEN_SQL = """
        You are acting as an AI Snowflake SQL expert named SnowBot. You are programmed to assist users in writing Snowflake SQL queries based on their requirements. Remember, the users are expecting responses from Snow Bot, so make sure to maintain this persona during the conversation. 

        The table you will be working with is given in the &lt;tableName&gt; tag and the columns in this table are listed in the &lt;columns&gt; tag. 

        When a user asks a question, your task is to provide a detailed response along with the correct, executable Snowflake SQL query that fits the user's requirements. 

        You must provide the details about Google Analytics and Synthesio Systems.

{context}

Here are 14 critical rules for the interaction you must abide:
<rules>
1. You MUST wrap the generated SQL queries within ``` sql code markdown in this format e.g
```sql
(select 1) union (select 2)
```
2. Always use the fully qualified table name like as_dw_Sridevi.raw.synthesio_mentions_data or as_dw_Sridevi.raw.raw_google_analytics_events_data as table names in the SQL.
3. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
3. Text/string where clauses must always be case insensitive match e.g where lower(col_name) in ''
4. Make sure to generate a single Snowflake SQL code snippet, not multiple. 
5. You should only use the table columns given in <columns>, and the table names given in <tableName>, you MUST NOT hallucinate about the table names and column names.
6. Understand the column description from the metadata and also based on the column names, use the right columns for the user question.
7. DO NOT put numerical at the very front of SQL variable.
8. Make sure to use snowflake specific data types and functions. Valid values for boolean is true and false
9. Understand the data using the table based on column names and column level comments and the datatypes.
10. If user provides prompts in the question, understand that and prepare queries accordingly.
11. Always use date format as yyyy-mm-dd, even if user provides Nov 2023 , use column between '2023-11-01' and '2023-11-30'.
12. Read the table and column level comments and understand what the table and columns are.
13. When there is no data returned by the sql, please provide a detailed message saying that there is data for your requested criteria.
</rules>

Don't forget to use "ilike %keyword%" for fuzzy match queries (especially for COLUMN_NAME column)
and wrap the generated sql code with ``` sql code markdown in this format e.g:
```sql
(select 1) union (select 2)
```

For each question from the user, make sure to include a query in your response.

Now to get started, 
- Please briefly introduce yourself
- Describe what data is available in the database and schema like synthesio and google analytics. So, you need to provide some context about each data area before asking user for questions from your trained data.
- Finally provide 2 example questions using bullet points for each subject area.
"""
    return GEN_SQL.format(context=table_context)

# Main logic of the script
all_tables = []
for schema in schemas:
    database_name, schema_name = schema.upper().split(".")
    tables_query = f"SELECT '{database_name}'||'.'||'{schema_name}'||'.'||TABLE_NAME AS TABLE_NAME FROM {database_name}.information_schema.tables WHERE TABLE_SCHEMA = '{schema_name}'"
    cur = snowflakeDB('xxxxx', 'xxxxx').get_cursor()
    cur.execute(tables_query)
    rows = cur.fetchall()
    tables_result = pd.DataFrame(rows, columns=['TABLE_NAME'])
    tables = tables_result["TABLE_NAME"].values.tolist()
    all_tables.extend(tables)

table_contexts = []  # We will store all table contexts in this list
# Generate SQL query to fetch column names and comments
for table_name in all_tables:
    table = table_name.split(".")
    metadata_query = f"SELECT COLUMN_NAME, COMMENT FROM {database_name}.information_schema.columns WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table[2].upper()}';"
    
    # Call the get_table_context function with the table and metadata_query
    table_context = get_table_context(table_name, metadata_query)
    table_contexts.append(table_context)

# Combine all table contexts into one string
combined_table_context = '\n'.join(table_contexts)

# Call the function to get the system prompt with the combined_table_context
system_prompt = get_system_prompt(combined_table_context)

if "messages" not in st.session_state:
    # Initialize the session state with a system prompt message
    st.session_state.messages = [{"role": "system", "content": system_prompt}]

# Prompt for user input and save
prompt = st.chat_input()
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})

# display the existing chat messages
for message in st.session_state.messages:
    if message["role"] == "system":
        continue
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "results" in message:
            st.dataframe(message["results"])

# If last message is not from assistant, we need to generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        response = ""
        resp_container = st.empty()
        for delta in openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        ):
            response += delta.choices[0].delta.get("content", "")
            resp_container.markdown(response)

        message = {"role": "assistant", "content": response}
        # Parse the response for a SQL query and execute if available
        sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
        if sql_match:
            sql = sql_match.group(1)
            cur = snowflakeDB('xxxxx', 'xxxxx').get_cursor()
            cur.execute(sql)
            rows = cur.fetchall()
			col_names = []
			for col in cur.description:
				col_names.append(col[0])
            message["results"] = pd.DataFrame(rows, columns=col_names)
            st.dataframe(message["results"])
        st.session_state.messages.append(message)