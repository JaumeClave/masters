import psycopg2
from psycopg2 import Error
import streamlit as st
from multipage import MultiPage
from pages import add_golf_round, add_golf_course, authentication, dashboard

# Variables
USER = st.secrets["postgres"]["user"]
PASSWORD = st.secrets["postgres"]["password"]
DATABASE = st.secrets["postgres"]["dbname"]
HOST = st.secrets["postgres"]["host"]


# Functions
@st.cache(allow_output_mutation=True)
def connect_to_postgres_database(user, password, database, host="127.0.0.1", port="5432"):
    """
    Function connects to a database and returns the cursor object
    :param user: database username
    :param password: database password
    :param database: database name
    :param host: server location
    :param port: listening port
    :return: psycopg2 cursor object
    """
    try:
        con = psycopg2.connect(user=user,
                               password=password,
                               database=database,
                               host=host,
                               port=port)
        cursor = con.cursor()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    return con, cursor

############################################ STREAMLIT ############################################


con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE, HOST,
                                           port="5432")

# Create an instance of the app
app = MultiPage()

# Add all your applications (pages) here
app.add_page("Login / Signup", authentication.app)
app.add_page("View Data", dashboard.app)
app.add_page("Upload Golf Round", add_golf_round.app)
app.add_page("Add a New Course", add_golf_course.app)

# The main app
app.run()





