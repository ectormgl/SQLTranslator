from dotenv import load_dotenv
import os
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
import mysql.connector
from sqlalchemy import create_engine
import streamlit as st
from langchain_mistralai import ChatMistralAI 
import pandas as pd

st.set_page_config(page_title="Chat com MySQL", page_icon=":speech_balloon:")
st.title("Converse com seu banco de dados, SQLTranslator!")
useCloud = False

def get_sql_chain(db):
    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, write a SQL query that would answer the user's question. Take the conversation history into account.
    
    <SCHEMA>{schema}</SCHEMA>
    
    Conversation History: {chat_history}
    
    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.
    
    For example:
    Question: which 3 artists have the most tracks?
    SQL Query: SELECT ArtistId, COUNT(*) as track_count FROM Track GROUP BY ArtistId ORDER BY track_count DESC LIMIT 3;
    Question: Name 10 artists
    SQL Query: SELECT Name FROM Artist LIMIT 10;
    
    Your turn:
    
    Question: {question}
    SQL Query:
    """
    prompt = ChatPromptTemplate.from_template(template)
    model = ChatMistralAI()
    
    def get_schema(_):
        return db.get_table_info()
    
    return (
        RunnablePassthrough.assign(schema=get_schema)
        | prompt
        | model
        | StrOutputParser()
    )

def init_database(db_type, uri=None, user=None, password=None, host=None, port=None, database=None):
    try:
        if uri:
            db_uri = uri
        else:
            if db_type == "MySQL":
                db_uri = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
            elif db_type == "PostgreSQL":
                db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            elif db_type == "Oracle":
                db_uri = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}"
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
        
        print(f"Attempting to connect with URI: {db_uri}")
        return SQLDatabase.from_uri(db_uri)
    except Exception as e:
        raise ConnectionError(f"Failed to initialize {db_type} database: {str(e)}")

def execute_query(db_params, query):
    connection = mysql.connector.connect(**db_params)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
        return pd.DataFrame(results, columns=columns)
    finally:
        connection.close()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        AIMessage(content="Hello! I'm a SQL assistant. Ask me anything about your database."),
    ]

load_dotenv()

with st.sidebar:
    st.subheader("Settings")
    st.write("Esse é um simples chat utilizando uma LLM (Large Language Model) para responder a perguntas sobre o seu banco de dados.")

    # Database type selection
    db_type = st.selectbox("Select Database Type", ["MySQL", "PostgreSQL", "Oracle"])
    cloud_server= st.checkbox("Usar banco de dados remoto")

    # Toggle for using URI or manual inputs
    use_uri = st.checkbox("Use URI for connection")
    if cloud_server:
        useCloud = True
        st.write("Ao marcar essa opção, você irá usar o banco de dados remoto oferecido pela SQLTranslator")
    elif use_uri:
        # Show only the URI input
        placeholder_uri = {
            "MySQL": "mysql+mysqlconnector://user:password@host:port/database",
            "PostgreSQL": "postgresql://user:password@host:port/database",
            "Oracle": "oracle+cx_oracle://user:password@host:port/database"
        }
        db_uri = st.text_input("Database URI", value=placeholder_uri[db_type], key="db_uri")
    else:
        # Show individual inputs for manual connection
        st.text_input("Host", value="localhost", key="Host")
        st.text_input("Port", value="3307" if db_type == "MySQL" else "5432" if db_type == "PostgreSQL" else "1521", key="Port")
        st.text_input("User", value="root", key="User")
        st.text_input("Password", type="password", value="mysql", key="Password")
        st.text_input("Database", value="world", key="Database")
    
    # Connection button
    if st.button("Connect"):
        with st.spinner(f"Connecting to {db_type} database..."):
            try:
                if useCloud:
                    db = init_database(db_type, uri=os.getenv('POSTGRE_URI'))

                if use_uri:
                    db = init_database(db_type, uri=st.session_state["db_uri"])
                    # Extract connection parameters from URI for future use
                    engine = create_engine(st.session_state["db_uri"])
                    db_params = engine.url.translate_connect_args()
                else:
                    db = init_database(
                        db_type,
                        user=st.session_state["User"],
                        password=st.session_state["Password"],
                        host=st.session_state["Host"],
                        port=st.session_state["Port"],
                        database=st.session_state["Database"]
                    )
                    db_params = {
                        "user": st.session_state["User"],
                        "password": st.session_state["Password"],
                        "host": st.session_state["Host"],
                        "port": st.session_state["Port"],
                        "database": st.session_state["Database"],
                    }
                
                st.session_state.db = db
                st.session_state.db_params = db_params
                st.success(f"Connected to {db_type} database!")
            except ConnectionError as e:
                st.error(str(e))
            except SQLAlchemyError as e: # type: ignore
                st.error(f"SQLAlchemy error: {str(e)}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {str(e)}")

for message in st.session_state.chat_history:
    if isinstance(message, AIMessage):
        with st.chat_message("AI", avatar=":material/smart_toy:" ):
            st.markdown(message.content)
    elif isinstance(message, HumanMessage):
        with st.chat_message("Human"):
            st.markdown(message.content)

user_query = st.chat_input("Type a message...")
if user_query is not None and user_query.strip() != "":
    st.session_state.chat_history.append(HumanMessage(content=user_query))
    with st.chat_message("Human"):
        st.markdown(user_query)
    
    with st.chat_message("AI", avatar=":material/smart_toy:"):
        sql_chain = get_sql_chain(st.session_state.db)
        sql_query = sql_chain.invoke({
            "chat_history": st.session_state.chat_history,
            "question": user_query
        })
        
        st.markdown(f"Generated SQL Query:")
        st.code(sql_query, language="sql")
        
        try:
            results_df = execute_query(st.session_state.db_params, sql_query)
            st.markdown("Query Results:")
            st.dataframe(results_df)
            
            response = f"I've executed the query and displayed the results above. Is there anything else you'd like to know about the data?"
        except Exception as e:
            response = f"An error occurred while executing the query: {str(e)}"
        
        st.markdown(response)
        st.session_state.chat_history.append(AIMessage(content=response))

print(st.session_state)