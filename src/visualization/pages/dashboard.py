import psycopg2
from pylab import *
import calplot
from plotly_calplot import calplot
import pandas as pd
import streamlit as st
import plotly.express as px
from psycopg2 import Error
from sqlalchemy import create_engine


# Variables
USER = st.secrets["postgres"]["user"]
PASSWORD = st.secrets["postgres"]["password"]
DATABASE = st.secrets["postgres"]["dbname"]
HOST = st.secrets["postgres"]["host"]
ROUND_COURSE_COUNTRY_PLAYED_TEXT = "You've played {} rounds across {} golf courses in {} different countries."
BETTER_FRONT9_TEXT = "You get off to a hot start!"
BETTER_BACK9_TEXT = "You are able to finish your round well!"
SAME_FRONT_BACK9_TEXT = "You are consistent throughout your round!"
ROUND_AVERAGE_SCORE_TEXT = "For par 72 courses, your average 18 hole score is {}. From these rounds, your average front 9 score is {} and your back 9 score is {}. {}"
BEST_ROUND_TEXT = "You played your best round at {} ({}, {}) on {}. You shot a {} ({}{})."
RECENT_ROUND_TEXT = "The last round you played was on {} at {} ({}, {}). You shot a {} ({}{})."
HANDICAP_TEXT = "Your handicap index is **{}**. As defined by the World Handicap System, " \
                "this has been calculated using the best {} scores from your {} most recent rounds."
HANDICAP_TEXT_ERROR = "You need a minimum of three rounds to calculate your handicap index. You've played {}."



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
                         hover_name="country", size="count", projection="natural earth",
                         width=750, height=400)
    fig.update_layout(legend={'title_text':''})
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


def make_daily_rounds_date_played_df(rounds_date_played_df):
    """
    Function creates a dataframe with index date and a column showing count of rounds played per date
    :param rounds_date_played_df: dataframe with round id and the date that round was played
    :return: dataframe with index date and a column showing count of rounds played per date
    """
    rounds_date_played_df["date_played"] = pd.to_datetime(rounds_date_played_df["date_played"])
    dms = rounds_date_played_df.groupby(rounds_date_played_df['date_played'].dt.to_period('D')).count()['round_id'].to_timestamp()
    max_year = rounds_date_played_df['date_played'].dt.to_period('D').max().year
    min_year = rounds_date_played_df['date_played'].dt.to_period('D').min().year
    idx = pd.date_range(str(min_year) + '-1-1', str(max_year) + '-12-31')
    dms.index = pd.DatetimeIndex(dms.index)
    daily_rounds_date_played = dms.reindex(idx, fill_value=0)
    date_played_df = pd.DataFrame(daily_rounds_date_played).reset_index()
    return date_played_df


def make_plotly_calendar_map(date_played_df):
    """
    Function returns a calplot plotly object which allows for an interactive calendar
    :param date_played_df: dataframe with index date and a column showing count of rounds played per date
    :return: plotly calplot
    """
    fig = calplot(
        date_played_df, x="index", y="round_id", years_title=True, colorscale="greens", gap=0,
        name="Played", month_lines_width=1, month_lines_color="#79a883",
        space_between_plots=0.25)
    return fig


def pipeline_plot_rounds_date_played_calendar(user_id):
    """
    Function pipelines the functions required to return a calplot object showing the dates a
    user has played rounds
    :param user_id: id of user
    :return: None
    """
    rounds_date_played_df = make_sql_rounds_date_played_df(user_id)
    daily_rounds_date_played = make_daily_rounds_date_played_df(rounds_date_played_df)
    fig = make_plotly_calendar_map(daily_rounds_date_played)
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


def make_sql_best_round_score(user_id):
    """
    Function returns best round date played/score, course name/city/country/par and shots over/under
    :param user_id: id of user
    :return: date played score, course name, city, country, par and shots over/under
    """
    insert_command = """SELECT t.date_played, t.name, t.city, t.country, t.round_score, t.shots_over_under
                        FROM (
                            SELECT round.user_id, round.round_id, round.date_played, c.name, c.city, c.country, c.par, COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                                COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                                COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                                COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) AS round_score,
                                ( COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                                COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                                COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                                COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) - c.par ) AS shots_over_under
                            FROM round
                            JOIN course c on round.course_id = c.course_id
                            JOIN round_shots rs on round.round_id = rs.round_id) AS t
                        WHERE shots_over_under = (
                            SELECT MIN(shots_over_under)
                            FROM (
                                SELECT
                                    ( COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                                    COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                                    COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                                    COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) - c.par ) AS shots_over_under
                                FROM round
                                JOIN course c on round.course_id = c.course_id
                                JOIN round_shots rs on round.round_id = rs.round_id) AS t2
                            )
                        AND t.user_id = %s;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    best_round_tuple = returned_value[0]
    return best_round_tuple


def suffix(day):
    """
    Function adds the appropriate suffix to a day
    :param day: the day (number) of the month
    :return: the day (number) with the correct suffix
    """
    return 'th' if 11 <= day <= 13 else {1:'st', 2:'nd', 3:'rd'}.get(day%10, 'th')


def custom_strftime(datetime_object):
    """
    Function returns a datetime object in the string format of month day(st/nd/rd/th), year
    :param datetime_object: datetime object
    :return: string format of month day(st/nd/rd/th), year
    """
    return datetime_object.strftime('%B {S}, %Y').replace('{S}', str(datetime_object.day) + suffix(datetime_object.day))


def make_round_variables(best_round_tuple):
    """
    Function returns six variables by splitting the tuple provided to the function
    :param best_round_tuple: tuple containing features of the round played
    :return:
    """
    date_played = custom_strftime(best_round_tuple[0])
    course_name = best_round_tuple[1]
    course_city = best_round_tuple[2]
    course_country = best_round_tuple[3]
    round_score = best_round_tuple[4]
    round_over_under_par = best_round_tuple[5]
    return date_played, course_name, course_city, course_country, round_score, round_over_under_par


def make_best_round_text(course_name, course_city, course_country, date_played, round_score, round_over_under_par):
    """
    Function returns the best round text
    :param course_name: name of course
    :param course_city: city of course
    :param course_country: country of course
    :param date_played: date played round
    :param round_score: score of round
    :param round_over_under_par: shots over/under par
    :return: text containing information about the best round played
    """
    if round_over_under_par >= 0:
        sign = "+"
    elif round_over_under_par < 0:
        sign = "-"
    else:
        pass
    text = BEST_ROUND_TEXT.format(course_name, course_city, course_country, date_played, round_score, sign, round_over_under_par)
    return text


def pipeline_make_best_round_text(user_id):
    """
    Function pipelines the process require to return the best round text
    :param user_id: id of user
    :return: text containing information about the best round played
    """
    best_round_tuple = make_sql_best_round_score(user_id)
    date_played, course_name, course_city, course_country, round_score, round_over_under_par = make_round_variables(
        best_round_tuple)
    text = make_best_round_text(course_name, course_city, course_country, date_played, round_score, round_over_under_par)
    return text


def make_sql_recent_round_score(user_id):
    """
    Function returns the most recent round date played/score, course name/city/country/par and shots over/under
    :param user_id: id of user
    :return: date played score, course name, city, country, par and shots over/under
    """
    insert_command = """SELECT round.date_played, c.name, c.city, c.country, COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) AS round_score,
                            ( COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) - c.par ) AS shots_over_under
                        FROM round
                        JOIN course c on round.course_id = c.course_id
                        JOIN round_shots rs on round.round_id = rs.round_id
                        WHERE date_played = (
                            SELECT MAX(date_played)
                            FROM round
                            )
                        AND round.user_id = 21;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    recent_round_tuple = returned_value[0]
    return recent_round_tuple


def make_recent_round_text(course_name, course_city, course_country, date_played, round_score, round_over_under_par):
    """
    Function returns the most recent round text
    :param course_name: name of course
    :param course_city: city of course
    :param course_country: country of course
    :param date_played: date played round
    :param round_score: score of round
    :param round_over_under_par: shots over/under par
    :return: text containing information about the the most recent round played
    """
    if round_over_under_par >= 0:
        sign = "+"
    elif round_over_under_par < 0:
        sign = "-"
    else:
        pass
    text = RECENT_ROUND_TEXT.format(date_played, course_name, course_city, course_country, round_score, sign, round_over_under_par)
    return text


def pipeline_make_recent_round_text(user_id):
    """
    Function pipelines the process require to return the most recent round text
    :param user_id: id of user
    :return: text containing information about the the most recent round played
    """
    best_round_tuple = make_sql_recent_round_score(user_id)
    date_played, course_name, course_city, course_country, round_score, round_over_under_par = make_round_variables(best_round_tuple)
    text = make_recent_round_text(course_name, course_city, course_country, date_played, round_score, round_over_under_par)
    return text


def make_sql_user_handicap(user_id):
    """
    Function returns the most recent entry for a users handicap index by calling the dashboard user handicap table
    :param user_id: id of user
    :return: current handicap index
    """
    insert_command = """SELECT handicap_index
                        FROM dashboard_user_handicap
                        WHERE handicap_id = (
                            SELECT MAX(handicap_id)
                            FROM dashboard_user_handicap
                            WHERE user_id = %s);"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    handicap_index = returned_value[0][0]
    return handicap_index


def make_sql_count_of_rounds_played(user_id):
    """
    Function returns the number of rounds in the round_shots table created by a specified user id
    :param user_id: id of user
    :return: count of rounds played
    """
    insert_command = """SELECT COUNT(rs.round_id)
                        FROM round
                        JOIN round_shots rs on round.round_id = rs.round_id
                        WHERE round.user_id = %s;"""
    cursor.execute(insert_command, [user_id])
    returned_value = cursor.fetchall()
    rounds_played = returned_value[0][0]
    return rounds_played


def make_sql_rounds_played_calculation_hcp_index(rounds_played):
    """
    Function returns the appropriate values for rounds played, rounds to be used and adjustment from the calculation_handicap_index table
    :param rounds_played: number of rounds played
    :return: rounds played, rounds to be used and adjustment
    """
    insert_command = """SELECT *
                        FROM calculation_handicap_index
                        WHERE number_of_rounds = %s"""
    cursor.execute(insert_command, [rounds_played])
    returned_value = cursor.fetchall()
    rounds_considered = returned_value[0][0]
    rounds_to_be_used = returned_value[0][1]
    adjustment = returned_value[0][2]
    return rounds_considered, rounds_to_be_used, adjustment


def make_logic_round_played_calculation_hcp_index_tuple(rounds_played):
    """
    Function creates the logic requried to return the correct round played tuple from the calculation_handicap_index tuple
    :param rounds_played: number of rounds played
    :return: rounds played, rounds to be used and adjustment
    """
    if rounds_played < 3:
        return (np.nan, np.nan, np.nan)
    elif rounds_played > 20:
        rounds_played = 20
        return make_sql_rounds_played_calculation_hcp_index(rounds_played)
    else:
        return make_sql_rounds_played_calculation_hcp_index(rounds_played)


def make_handicap_text(user_id, handicap_index, rounds_to_be_used, rounds_considered):
    """
    Function returns the full handicap text for a user
    :param handicap_index: user handicap
    :param rounds_considered: rounds considered for handicap calculation
    :param rounds_to_be_used: rounds used in handicap calulcation
    :return: full handicap text for a user
    """
    rounds_played = make_sql_count_of_rounds_played(user_id)
    if rounds_played < 3:
        text = HANDICAP_TEXT_ERROR.format(rounds_played)
    else:
        text = HANDICAP_TEXT.format(handicap_index, rounds_to_be_used, rounds_considered)
    return text


def pipeline_make_handicap_text(user_id):
    """
    Function pipelines the process required to show a user the full handicap text for a user
    :param user_id: id of user
    :return: full handicap text for a user
    """
    handicap_index = make_sql_user_handicap(user_id)
    rounds_played = make_sql_count_of_rounds_played(user_id)
    rounds_considered, rounds_to_be_used, adjustment = make_logic_round_played_calculation_hcp_index_tuple(rounds_played)
    text = make_handicap_text(user_id, handicap_index, rounds_to_be_used, rounds_considered)
    return text



def make_all_rounds_played_df(user_id):
    """
    Function returns a dataframe containing the all ky course, rounds and other information
    :param user_id: id of user
    :return: dataframe with course name, par, shot, score differential, weather, date
    """
    insert_command = """SELECT course.name, course.par,
                            COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) AS shots,
                            ( COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) - course.par) AS over_under,
                            TRUNC(( (113 / course.slope) * ((COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0)) - course.rating) )) AS score_differential,
                            COALESCE(rp.hole1,null) + COALESCE(rp.hole2,null) + COALESCE(rp.hole3,null) +
                            COALESCE(rp.hole4,null) +COALESCE(rp.hole5,null) + COALESCE(rp.hole6,null) + COALESCE(rp.hole7,null) + COALESCE(rp.hole8,null) +
                            COALESCE(rp.hole9,null) + COALESCE(rp.hole10,null) + COALESCE(rp.hole11,null) + COALESCE(rp.hole12,null) + COALESCE(rp.hole13,null) +
                            COALESCE(rp.hole14,null) + COALESCE(rp.hole15,null) + COALESCE(rp.hole16,null) + COALESCE(rp.hole17,null) + COALESCE(rp.hole18,null) AS putts,
                            ( COALESCE(rg.hole1,null) + COALESCE(rg.hole2,null) + COALESCE(rg.hole3,null) +
                            COALESCE(rg.hole4,null) +COALESCE(rg.hole5,null) + COALESCE(rg.hole6,null) + COALESCE(rg.hole7,null) + COALESCE(rg.hole8,null) +
                            COALESCE(rg.hole9,null) + COALESCE(rg.hole10,null) + COALESCE(rg.hole11,null) + COALESCE(rg.hole12,null) + COALESCE(rg.hole13,null) +
                            COALESCE(rg.hole14,null) + COALESCE(rg.hole15,null) + COALESCE(rg.hole16,null) + COALESCE(rg.hole17,null) + COALESCE(rg.hole18,null) ) AS gir,
                            ( COALESCE(rf.hole1,null) + COALESCE(rf.hole2,null) + COALESCE(rf.hole3,null) +
                            COALESCE(rf.hole4,null) +COALESCE(rf.hole5,null) + COALESCE(rf.hole6,null) + COALESCE(rf.hole7,null) + COALESCE(rf.hole8,null) +
                            COALESCE(rf.hole9,null) + COALESCE(rf.hole10,null) + COALESCE(rf.hole11,null) + COALESCE(rf.hole12,null) + COALESCE(rf.hole13,null) +
                            COALESCE(rf.hole14,null) + COALESCE(rf.hole15,null) + COALESCE(rf.hole16,null) + COALESCE(rf.hole17,null) + COALESCE(rf.hole18,null) ) AS fir,
                            round.temperature, round.humidity, round.wind_speed, round.date_played
                        FROM course
                        INNER JOIN round ON course.course_id = round.course_id
                        JOIN round_shots rs on round.round_id = rs.round_id
                        LEFT JOIN round_putts rp on round.round_id = rp.round_id
                        LEFT JOIN round_gir rg on round.round_id = rg.round_id
                        LEFT JOIN round_fir rf on round.round_id = rf.round_id
                        WHERE round.user_id=%(user_id)s"""
    all_rounds_df = pd.read_sql_query(insert_command, con=engine, params={"user_id": user_id})
    all_rounds_df[["score_differential", "putts", "gir", "fir"]] = all_rounds_df[["score_differential", "putts", "gir", "fir"]].astype("Int64")
    return all_rounds_df


def make_all_rounds_played_plotly(all_rounds_df):
    """
    Function uses all round data to output a time vs score chart
    :param all_rounds_df: dataframe with course name, par, shot, score differential, weather, date
    :return: plotly object
    """
    color_discrete_map = {"72": '#064e25', "71": "#0b9b49", "70": "#85cda4"}
    fig = px.scatter(
        data_frame=all_rounds_df,
        x="date_played",
        y="shots",
        color=all_rounds_df["par"].astype(str),
        custom_data=["name", 'par', 'over_under', "score_differential"], color_discrete_map=color_discrete_map,
        template="simple_white",
        labels=dict(date_played="", shots="Round Score"), size=list(all_rounds_df["score_differential"]))
    fig.update_traces(
        hovertemplate="<br>".join([
            "<b>%{customdata[0]}</b><br>",
            "Shots: %{y}",
            "Over/Under: %{customdata[2]}",
            "Differential: %{customdata[3]}",
            "Date: %{x}"]))
    fig.update_layout(legend_title_text='Course Par')
    fig.update_layout({"plot_bgcolor": "rgba(0, 0, 0, 0)", "paper_bgcolor": "rgba(0, 0, 0, 0)"})
    return fig


def pipeline_make_all_rounds_played_plotly(user_id):
    """
    Function pipelines the process required to out put a visualisation of the scores over time
    :param user_id: id of user
    :return: plotly fig
    """
    all_rounds_df = make_all_rounds_played_df(user_id)
    fig = make_all_rounds_played_plotly(all_rounds_df)
    return fig


########################################### STREAMLIT ################################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE, HOST, port="5432")
engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" + DATABASE)


def app():
    """

    :return:
    """
    st.subheader("Golfing profile 🥇")
    try:
        user_id = st.session_state["user_id"]
        user_id is not None
        # user_id = 21
        # Handicap index
        st.write(pipeline_make_handicap_text(user_id))
        # Rounds/Courses/Countries played text
        st.write(pipeline_make_total_rounds_courses_countries_text(user_id))
        # Average round score
        st.write(pipeline_make_average_round_score_text(user_id))
        # Best round information
        st.write(pipeline_make_best_round_text(user_id))
        # Best recent information
        st.write(pipeline_make_recent_round_text(user_id))
        # All rounds
        st.write("")
        st.subheader("Golf round data")
        st.write("This table shows all your rounds and relevant course and weather condition "
                 "data. You can sort by clicking column header.")
        st.dataframe(make_all_rounds_played_df(user_id))
        # Rounds played around the world
        st.write("")
        st.subheader("Rounds around the world")
        st.write("This map shows in what places you have played golf. Hover above color to view country.")
        st.plotly_chart(pipeline_plot_rounds_played_world_map(user_id))
        # Rounds played calendar
        st.subheader("Rounds over time")
        st.write("This calendar shows what days you have played golf over the years.")
        st.plotly_chart(pipeline_plot_rounds_date_played_calendar(user_id))
        # Scores over time
        st.subheader("Scores over time")
        st.write("This visualisation shows your round score across time. Hover on of the data to get more information into your round.")
        st.plotly_chart(pipeline_make_all_rounds_played_plotly(user_id))
    except KeyError:
        st.warning("You must login before accessing this page. Please authenticate via the login menu.")
    return None

#%%
