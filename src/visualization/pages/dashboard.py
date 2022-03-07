import psycopg2
from pylab import *
import calplot
import pandas as pd
import streamlit as st
import plotly.express as px
from psycopg2 import Error
from sqlalchemy import create_engine


# Variables
USER = "postgres"
PASSWORD = "Barca2011"
DATABASE = "golf_dashboard_db"
ROUND_COURSE_COUNTRY_PLAYED_TEXT = "You've played {} rounds across {} golf courses in {} different countries."
BETTER_FRONT9_TEXT = "You get off to a hot start!"
BETTER_BACK9_TEXT = "You are able to finish your round well!"
SAME_FRONT_BACK9_TEXT = "You are consistent throughout your round!"
ROUND_AVERAGE_SCORE_TEXT = "For par 72 courses, your average 18 hole score is {}. From these rounds, your average front 9 score is {} and your back 9 score is {}. {}"


# Functions
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


############################################## 08_view_data #####################################################


# Plot World Map
def make_sql_rounds_played_in_country_df(user_id):
    """
    Function returns a dataframe containing the country, alpha3, continent and times played in country for a specified user
    :param user_id: id of user
    :return: dataframe with country, alpha3, continent and times played in country for a specified user
    """
    insert_command = """SELECT ac.country, ac.count, country_code.alpha3, country_code.continent
                    FROM country_code
                    JOIN (SELECT country, count(country)
                        FROM (SELECT course.name, course.country, round.course_id
                            FROM course
                            INNER JOIN round on course.course_id = round.course_id
                            WHERE course.user_id=%(user_id)s) AS ac
                    GROUP BY country) ac
                    ON country_code.country = ac.country;"""
    played_in_country_df = pd.read_sql_query(insert_command, con=engine, params={"user_id": user_id})
    return played_in_country_df


def plot_rounds_played_world_map(rounds_played_country_df):
    """
    Function returns a plotly map object showing the countries a player has played in
    :param rounds_played_country_df: DataFrame containing country, alpha3, continent and times played in country for a specified user
    :return: scatter_geo object
    """
    fig = px.scatter_geo(rounds_played_country_df, locations="alpha3", color="continent",
                         hover_name="country", size="count", projection="natural earth")
    return fig


def pipeline_plot_rounds_played_world_map(user_id):
    """
    Function pipelines the functions required to return a plotly map object showing the countries a player has played in
    :param user_id: id of user
    :return: scatter_geo object
    """
    played_in_country_df = make_sql_rounds_played_in_country_df(user_id)
    fig = plot_rounds_played_world_map(played_in_country_df)
    return fig


# Plot Calendar
def make_sql_rounds_date_played_df(user_id):
    """
    Function returns a dataframe containing the round id and the date that round was played
    :param user_id: id of user
    :return: dataframe with round id and the date that round was played
    """
    insert_command = """SELECT round_id, date_played
                    FROM round
                    WHERE user_id=%(user_id)s
                    GROUP BY round_id;"""
    rounds_date_played_df = pd.read_sql_query(insert_command, con=engine, params={"user_id": user_id})
    return rounds_date_played_df


def make_daily_rounds_date_played_series(rounds_date_played_df):
    """
    Function creates a series with index date and a column showing count of rounds played per date
    :param rounds_date_played_df: dataframe with round id and the date that round was played
    :return: series with index date and a column showing count of rounds played per date
    """
    rounds_date_played_df["date_played"] = pd.to_datetime(rounds_date_played_df["date_played"])
    dms = rounds_date_played_df.groupby(rounds_date_played_df['date_played'].dt.to_period('D')).count()['round_id'].to_timestamp()
    max_year = rounds_date_played_df['date_played'].dt.to_period('D').max().year
    min_year = rounds_date_played_df['date_played'].dt.to_period('D').min().year
    idx = pd.date_range(str(min_year) + '-1-1', str(max_year) + '-12-31')
    dms.index = pd.DatetimeIndex(dms.index)
    daily_rounds_date_played = dms.reindex(idx, fill_value=0)
    return daily_rounds_date_played


def plot_rounds_date_played_calendar(daily_rounds_date_played):
    """
    Function uses the calplot library to plot a calendar containing dates rounds were played
    :param daily_rounds_date_played: series with index date and a column showing count of rounds played per date
    :return: None
    """
    cmap = cm.get_cmap("Greens", 10)
    fig, ax = calplot.calplot(daily_rounds_date_played, cmap = cmap)
    return fig


def pipeline_plot_rounds_date_played_calendar(user_id):
    """
    Function pipelines the functions required to return a calplot object showing the dates a user has played rounds
    :param user_id: id of user
    :return: None
    """
    rounds_date_played_df = make_sql_rounds_date_played_df(user_id)
    daily_rounds_date_played = make_daily_rounds_date_played_series(rounds_date_played_df)
    fig = plot_rounds_date_played_calendar(daily_rounds_date_played)
    return fig


# Make round/course/country played text
def make_sql_total_rounds_played(user_id):
    """
    Function queries the round table and returns the amount of rounds inputted/played by user
    :param user_id: id of user
    :return: rounds played
    """
    insert_command = """SELECT count(user_id)
                    FROM round
                    WHERE user_id=%s;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    rounds_played = returned_value[0][0]
    return rounds_played


def make_sql_total_courses_played(user_id):
    """
    Function queries the course table, joins with round and returns the amount of distinct courses a player has rounds for based on course_id
    :param user_id: id of user
    :return: courses played
    """
    insert_command = """SELECT count(DISTINCT name)
                        FROM (SELECT course.name
                            FROM course
                            INNER JOIN round on course.course_id = round.course_id
                            WHERE round.user_id=%s) as t_round_course;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    courses_played = returned_value[0][0]
    return courses_played


def make_sql_total_countries_played(user_id):
    """
    Function queries the course table, joins with round and returns the amount of distinct countries a player has rounds for based on course_id
    :param user_id: id of user
    :return: courses played
    """
    insert_command = """SELECT count(DISTINCT country)
                        FROM (SELECT course.country
                                FROM course
                                INNER JOIN round on course.course_id = round.course_id
                                WHERE round.user_id=%s) as t_round_country;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    countries_played = returned_value[0][0]
    return countries_played


def make_total_rounds_courses_countries(user_id):
    """
    Function pipelines the functions needed to return total rounds played, total distinct courses played and total distinct countries played
    :param user_id: id of user
    :return: rounds/courses/countries played
    """
    rounds_played = make_sql_total_rounds_played(user_id)
    courses_played = make_sql_total_courses_played(user_id)
    countries_played = make_sql_total_countries_played(user_id)
    return rounds_played, courses_played, countries_played


def pipeline_make_total_rounds_courses_countries_text(user_id):
    """
    Function pipelines the functions needed to a text to show total rounds played, total distinct courses played and total distinct countries played
    :return: text show total rounds/courses/countries played
    """
    rounds_played, courses_played, countries_played = make_total_rounds_courses_countries(user_id)
    text = ROUND_COURSE_COUNTRY_PLAYED_TEXT.format(rounds_played, courses_played, countries_played)
    return text


def make_sql_par_72_round_18_hole_average(user_id):
    """
    Function returns the average 18 hole score for all rounds played by a user in par 72 courses
    :param user_id: id of user
    :return: average round 18 hole score
    """
    insert_command = """SELECT AVG(total) AS all_round_avg
                        FROM (
                            SELECT COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) + COALESCE(rs.hole4,0) +
                            COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) +
                            COALESCE(rs.hole13,0) + COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) +
                            COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) AS total
                            FROM course
                            JOIN round ON course.course_id = round.course_id
                            JOIN round_shots rs on round.round_id = rs.round_id
                            WHERE course.par=72
                            AND round.user_id=%s)
                        AS t;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    avg_round_score = round(returned_value[0][0])
    return avg_round_score


def make_sql_par_72_round_front9_average(user_id):
    """
    Function returns the average front9 hole score for all rounds played by a user in par 72 courses
    :param user_id: id of user
    :return: average round front9 hole score
    """
    insert_command = """SELECT AVG(total) AS all_round_avg
                        FROM (
                            SELECT COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                                   COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                                   COALESCE(rs.hole9,0) AS total
                            FROM course
                            JOIN round ON course.course_id = round.course_id
                            JOIN round_shots rs on round.round_id = rs.round_id
                            WHERE course.par=72
                            AND round.user_id=%s)
                        AS t;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    avg_round_score = round(returned_value[0][0])
    return avg_round_score


def make_sql_par_72_round_back9_average(user_id):
    """
    Function returns the average back9 hole score for all rounds played by a user in par 72 courses
    :param user_id: id of user
    :return: average round back9 hole score
    """
    insert_command = """SELECT AVG(total) AS all_round_avg
                        FROM (
                            SELECT COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                                   COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) +
                                   COALESCE(rs.hole18,0) AS total
                            FROM course
                            JOIN round ON course.course_id = round.course_id
                            JOIN round_shots rs on round.round_id = rs.round_id
                            WHERE course.par=72
                            AND round.user_id=%s)
                        AS t;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    avg_round_score = round(returned_value[0][0])
    return avg_round_score


def make_par_72_18_front_back_9_averages(user_id):
    """
    Function pipelines the functions needed to return average 18 and front/back9 hole score
    :param user_id: id of user
    :return: average 18 and front/back9 hole score
    """
    avg_18_hole_round_score = make_sql_par_72_round_18_hole_average(user_id)
    avg_front9_hole_round_score = make_sql_par_72_round_front9_average(user_id)
    avg_back9_hole_round_score = make_sql_par_72_round_back9_average(user_id)
    return avg_18_hole_round_score, avg_front9_hole_round_score, avg_back9_hole_round_score


def make_front_back_9_comparison_text(avg_front9, avg_back9):
    """
    Function returns a text based on a players average front/back9 score
    :param avg_front9: average strokes on the front9
    :param avg_back9: average strokes on the back9
    :return: comparison text
    """
    if avg_front9 < avg_back9:
        return BETTER_FRONT9_TEXT
    elif avg_front9 > avg_back9:
        return BETTER_BACK9_TEXT
    elif avg_front9 == avg_back9:
        return SAME_FRONT_BACK9_TEXT


def pipeline_make_average_round_score_text(user_id):
    """
    Function pipelines the functions needed to a text to show total rounds played, total distinct courses played and total distinct countries played
    :return: text show total rounds/courses/countries played
    """
    avg_18_hole_round_score, avg_front9_hole_round_score, avg_back9_hole_round_score = make_par_72_18_front_back_9_averages(user_id)
    comparison_text = make_front_back_9_comparison_text(avg_front9_hole_round_score, avg_back9_hole_round_score)
    text = ROUND_AVERAGE_SCORE_TEXT.format(avg_18_hole_round_score, avg_front9_hole_round_score, avg_back9_hole_round_score, comparison_text)
    return text


########################################### STREAMLIT ################################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE, host="127.0.0.1",
                                           port="5432")
engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@localhost/" + DATABASE)


def app():
    """

    :return:
    """
    st.subheader("Golfing Profile")
    try:
        user_id = st.session_state["user_id"]
        user_id is not None
        # user_id = 1
        # Rounds/Courses/Countries played text
        st.write(pipeline_make_total_rounds_courses_countries_text(user_id))
        # Average round score
        st.write(pipeline_make_average_round_score_text(user_id))
        # Rounds played around the world
        st.write("Rounds around the world")
        st.plotly_chart(pipeline_plot_rounds_played_world_map(user_id))
        # Rounds played calendar
        st.write("Rounds across time")
        st.pyplot(pipeline_plot_rounds_date_played_calendar(user_id))
    except KeyError:
        st.warning("You must login before accessing this page. Please authenticate via the login menu.")
    return None

#%%
