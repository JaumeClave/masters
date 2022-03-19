import pandas as pd
import psycopg2
import streamlit as st
from psycopg2 import Error
from datetime import datetime


# Variables
USER = st.secrets["postgres"]["user"]
PASSWORD = st.secrets["postgres"]["password"]
DATABASE = st.secrets["postgres"]["dbname"]
HOST = st.secrets["postgres"]["host"]


# Initialise connection and generate cursor
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


# View queried table as dataframe
def table_to_dataframe(table):
    """
    Function returns a queried table in a Pandas DataFrame
    :param table: name of table in database
    :return: dataframe of table
    """
    cursor.execute("""SELECT * FROM {}""".format(table))
    tmp = cursor.fetchall()
    col_names = list()
    for elt in cursor.description:
        col_names.append(elt[0])
    df = pd.DataFrame(tmp, columns=col_names)
    return df


# Cursor execute command
def cursor_execute_tuple(command, data_tuple):
    """
    Function uses the cursor object to execute a command with a tuple pair. It commits and rollsback if error
    :param command: SQL query to be executed
    :param data_tuple: data pairing for SQL query variables
    :return:
    """
    try:
        cursor.execute(command, data_tuple)
        con.commit()
        print("Successfully executed the command")
    except:
        con.rollback()
        print("Could not successfully execute the command")
    return None


def make_date_time():
    """
    Function makes the current date and time
    :return: current date and time
    """
    date_created = datetime.today().date()
    time_created = datetime.now().time().strftime("%H:%M:%S")
    return date_created, time_created


# Insert course data into course table
def insert_course_in_course_table(name, user_id, holes_18, city, slope, rating, par, country):
    """
    Function inserts course information into the course table
    :param name: the name of the course (TEXT)
    :param holes_18: the number of holes the course has (INT)
    :param city: the city/location of the course (TEXT)
    :param slope: the slope of the course (FLOAT)
    :param rating: the rating of the course (FLOAT)
    :param par: the par of the course (INT)
    :param country: the country of the course (TEXT)
    :return:
    """
    insert_command = """INSERT INTO course
                  (name, user_id, holes_18, city, slope, rating, par, country, date_created,
                  time_created)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    date_created, time_created = make_date_time()
    data_tuple = (name, user_id, holes_18, city, slope, rating, par, country, date_created,
                  time_created)
    cursor_execute_tuple(insert_command, data_tuple)
    return None


def get_id_from_course_name(course_name):
    """
    Function returns the id of the course in the course table based on the name
    :param course_name: name of course quiered
    :return: id of course quiered
    """
    insert_command = """SELECT course_id FROM course
                    WHERE name = %s;"""
    cursor.execute(insert_command, [course_name])
    returned_value = cursor.fetchall()
    id = returned_value[0][0]
    return id


# Score card generate and download
def make_hole_number_range_scorecard(course_holes18):
    """
    Function creates a list of numbers. Either 9 holes or 18
    :param course_holes18: number of holes the course has
    :return: list of holes where number of holes is either 9 or 18
    """
    if course_holes18 == 9:
        list_of_holes = list(range(1, 10))
    elif course_holes18 == 18:
        list_of_holes = list(range(1, 19))
    else:
        pass
    return list_of_holes


def make_course_score_card_csv(course_name, course_holes18):
    """
    Function will generate a .csv containing the course name and number of holes
    :param course_name: name of course queried
    :param course_holes18: number of holes the course has
    :return: to_csv object that can be downloaded
    """
    course_score_card_template_df = pd.DataFrame()
    list_of_holes = make_hole_number_range_scorecard(course_holes18)
    course_score_card_template_df["Hole"] = list_of_holes
    course_score_card_template_df["Distance"] = ""
    course_score_card_template_df["Par"] = ""
    course_score_card_template_df["Stroke Index"] = ""
    course_score_card_template_df["Name"] = course_name
    course_score_card_template_df_csv = course_score_card_template_df.to_csv(index=False,
                                                                             encoding='latin1')
    return course_score_card_template_df_csv


# Insert course features par/distance/si
def make_data_tuple_9holes(table, course_feature, course_id):
    """
    Function creates the SQL command needed to insert features into a table - 9 holes
    :param table: name of table in database
    :param course_feature: list of features
    :param course_id: id of course in course table
    :return: insert SQL command, features as tuple
    """
    insert_command = """INSERT INTO {}
                  (course_id, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8, hole9)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(table)
    course_id_list = [course_id]
    course_id_tuple = tuple(course_id_list)
    feature_tuple = tuple(course_feature)
    data_tuple = tuple(course_id_tuple) + tuple(feature_tuple)
    return insert_command, data_tuple


def make_data_tuple_18holes(table, course_feature, course_id):
    """
    Function creates the SQL command needed to insert features into a table - 18 holes
    :param table: name of table in database
    :param course_feature: list of features
    :param course_id: id of course in course table
    :return: insert SQL command, features as tuple
    """
    insert_command = """INSERT INTO {}
                      (course_id, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8, hole9, hole10, hole11, hole12, hole13, hole14, hole15, hole16, hole17, hole18)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(
        table)
    course_id_list = [course_id]
    course_id_tuple = tuple(course_id_list)
    feature_tuple = tuple(course_feature)
    data_tuple = tuple(course_id_tuple) + tuple(feature_tuple)
    return insert_command, data_tuple


def insert_score_card_feature_to_table(table, course_feature, course_id):
    """
    Function pipelines the process to insert 9/18 hole feature information (par, si, distance) into a database table
    :param table: name of table in database
    :param course_feature: list of features
    :param course_id: id of course in course table
    :return:
    """
    if len(course_feature) == 9:
        insert_command, data_tuple = make_data_tuple_9holes(table, course_feature, course_id)
    elif len(course_feature) == 18:
        insert_command, data_tuple = make_data_tuple_18holes(table, course_feature, course_id)
    cursor_execute_tuple(insert_command, data_tuple)
    return None


def make_course_df_and_insert_course_feature(file_path, course_name):
    """
    Function will read a file path (CSV) and insert features to various course_feature tables
    :param file_path: path of file (CSV)
    :param course_name: name of course
    :return: None
    """
    course_id = get_id_from_course_name(course_name)
    dataframe = pd.read_csv(file_path)
    course_par = list(dataframe["Par"])
    course_distance = list(dataframe["Distance"])
    course_stroke_index = list(dataframe["Stroke Index"])
    insert_score_card_feature_to_table("course_par", course_par, course_id)
    insert_score_card_feature_to_table("course_distance", course_distance, course_id)
    insert_score_card_feature_to_table("course_stroke_index", course_stroke_index,
                                       course_id)
    return None


################################ STREAMLIT #######################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE, HOST, port="5432")

def app():
    """
    Function to render the add_golf_course.py page via the app.py file
    """
    st.subheader("Add a course ⛳")
    try:
        user_id = st.session_state["user_id"]
        user_id is not None
        # user_id = 1
        # Adding course instructions
        with st.expander("Click to learn how to add a course"):
            st.write("1. Populate all fields in this below")
            st.write("2. Click *'Add course and download course score card template'*")
            st.write("3. Fill in the downloaded score card with the course holes distance/par/stroke "
                     "index")
            st.write("4. Upload the filled out score card using the file uploader")
            st.write("")
        st.write("")
        # Course information
        course_name = st.text_input("Name")
        course_slope = st.number_input("Slope", step=0.1)
        course_rating = st.number_input("Rating", step=0.1)
        course_par = st.number_input("Par", step=1)
        course_holes18 = st.selectbox('Holes', (18, 9))
        course_city = st.text_input("City")
        course_country = st.text_input("Country")
        # Generate score card template
        if course_name != "":
                course_score_card_template_df_csv = make_course_score_card_csv(course_name,
                                                                           course_holes18)
        # Check if all course info fields are populated
        if course_name != "" and course_city != "" and course_holes18 != "" and course_slope != 0 \
                and course_rating != 0 and course_country != "" and course_par != 0:
            # Add course and download score card template button
            if st.download_button(
                    label="Add course and download course score card template",
                    data=course_score_card_template_df_csv,
                    file_name="golf_course_score_card_template.csv"):
                # Insert course in database
                try:
                    insert_course_in_course_table(course_name, user_id, course_holes18,
                                                  course_city,
                                                  course_slope, course_rating, course_par,
                                                  course_country)
                except:
                    pass
            # File uploader
            uploaded_file = st.file_uploader("Upload course score card")
            if uploaded_file is not None:
                try:
                    # Insert course to db
                    make_course_df_and_insert_course_feature(uploaded_file, course_name)
                    st.success("Successfully added course")
                    st.write("")
                    st.write("Thanks for adding a course to the database!")
                except:
                    st.error("We could not upload your file. Please make sure you are uploading "
                             "the *course score card template* downloaded above!")
    except KeyError:
        st.warning("You must login before accessing this page. Please authenticate via the login menu.")
    return None


