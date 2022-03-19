import psycopg2
import collections
import math
from datetime import datetime
from datetime import time
from statistics import mean
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
from geopy.geocoders import Nominatim
from meteostat import Hourly
from meteostat import Stations
from psycopg2 import Error


# Variables
USER = st.secrets["postgres"]["user"]
PASSWORD = st.secrets["postgres"]["password"]
DATABASE = st.secrets["postgres"]["dbname"]
HOST = st.secrets["postgres"]["host"]
NOMINATIM_USER_AGENT = "James"
WEATHER_CODE_LIST = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27]
WEATHER_CONDITION_LIST = ["Clear", "Fair", "Cloudy", "Overcast", "Fog", "Freezing Fog", "Light Rain", "Rain", "Heavy Rain", "Freezing Rain", "Heavy Freezing Rain", "Sleet", "Heavy Sleet", "Light Snowfall", "Snowfall", "Heavy Snowfall", "Rain Shower", "Heavy Rain Shower", "Sleet Shower", "Heavy Sleet Shower", "Snow Shower", "Heavy Snow Shower", "Lightning", "Hail", "Thunderstorm", "Heavy Thunderstorm", "Storm"]
WEATHER_CODE_CONDITION_DICTIONARY = dict(zip(WEATHER_CODE_LIST, WEATHER_CONDITION_LIST))
WEATHER_CONDITION_TEXT = "Overall conditions - {}... "
PRECIPITATION_TEXT = "There were {} mm of rainfall. "
WIND_SPEED_TEXT = "Wind speed averaged {} km/h. "
HUMIDITY_LEVEL = "Humidity level of {}%. "
TEMPERATURE_TEXT = "It was {} degrees during your round. "
DISTANCE_TABLE = "course_distance"
PAR_TABLE = "course_par"
SI_TABLE = "course_stroke_index"
FRONT9 = "Front 9"
BACK9 = "Back 9"
ALL18 = "18"
ROUND_SHOTS_TABLE = "round_shots"
ROUND_PUTTS_TABLE = "round_putts"
ROUND_FIR_TABLE = "round_fir"
ROUND_GIR_TABLE = "round_gir"


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


def make_alphabetical_course_name_list():
    """
    Function returns the row for a course feature table based on the id passed
    :param course_id: id of course in course table
    :return: sorted list of courses in course table
    """
    insert_command = """SELECT name FROM course;"""
    cursor.execute(insert_command,)
    returned_value = cursor.fetchall()
    feature_list = list(returned_value)
    sorted_feature_list = sorted([i[0] for i in feature_list])
    return sorted_feature_list


def get_city_country_from_course_id(course_id):
    """
    Function returns the city and country features for a specified course id
    :param course_id: id of course queried
    :return: city and country
    """
    insert_command = """SELECT city, country FROM course
                    WHERE course_id = %s;"""
    cursor.execute(insert_command, [course_id])
    returned_value = cursor.fetchall()
    city = returned_value[0][0]
    country = returned_value[0][1]
    return city, country


# Get Lat & Lon from City/Country name
def make_lat_lon_for_city(city, country):
    """
    Function queries the Nominatim API to return the lat & Lon coordinates of a specified city & country
    :param city: name of the city
    :param country: name of the country the city is in
    :return: lat and lon coordinate pair of city geolocation
    """
    geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)
    loc = geolocator.geocode(city+','+ country)
    lat = loc.latitude
    lon = loc.longitude
    return lat, lon


# Get station id and weather conditions
def make_nearest_station_id_from_lon_lat(lat, lon):
    """
    Function returns the nearest weather (Meteostat) station id based on passed lat & lon coordinates
    :param lat: latitude coordinate
    :param lon: longitude coordinate
    :return: weather (Meteostat) station id
    """
    stations = Stations()
    stations = stations.nearby(lat, lon)
    stations = stations.fetch()
    all_station_ids = stations.index
    return all_station_ids


def make_historical_hourly_weather_df(station_id, start, end):
    """
    Function returns a Pandas DataFrame containing the hourly weather conditions for a specified station during a specified time
    :param station_id: id of Meteostat weather station
    :param start: start date/time
    :param end: end date/time
    :return: DataFrame containing hourly weather conditions
    """
    data = Hourly(station_id, start, end)
    data = data.fetch()
    return data


def make_pipeline_city_historical_hourly_weather_df(city, country, start, end):
    """
    Functions pipelines the process required to output a Pandas DataFrame containing the hourly weather conditions for a specified station during a specified time
    :param city: name of the city
    :param country: name of the country the city is i
    :param start: start date/time
    :param end: end date/time
    :return: DataFrame containing hourly weather conditions
    """
    lat, lon = make_lat_lon_for_city(city, country)
    all_station_ids = make_nearest_station_id_from_lon_lat(lat, lon)
    for index, value in enumerate(all_station_ids):
        data = make_historical_hourly_weather_df(all_station_ids[index], start, end)
        if len(data) != 0:
            break
    return data


def make_round_weather_condition_lists(weather_data):
    """
    Function returns lists of key features from the weather data
    :param weather_data: DataFrame containing hourly weather conditions
    :return: lists of key weather features
    """
    round_air_temp = list(weather_data["temp"])
    round_rhumidity = list(weather_data["rhum"])
    round_precipitation = list(weather_data["prcp"])
    round_wind_speed = list(weather_data["wspd"])
    round_weather_code = list(weather_data["coco"])
    return round_air_temp, round_rhumidity, round_precipitation, round_wind_speed, round_weather_code


def make_list_without_nan_values(list_of_values):
    """
    Function loops through a list and removes all np.nan values from it
    :param list_of_values: list of values
    :return: list of values without np.nan
    """
    clean_list = [i for i in list_of_values if np.isnan(i) == False]
    return clean_list


def make_mean_weather_feature(weather_feature_list):
    """
    Function returns a mean or np.nan value for a weather feature list depending on what it originally contains
    :param weather_feature_list: weather feature values to be averaged
    :return: averaged weather feature average (or np.nan)
    """
    if len(weather_feature_list) == 0:
        value = np.nan
    else:
        value = round(mean(weather_feature_list))
    return value


def make_sum_weather_feature(weather_feature_list):
    """
    Function returns a sum or np.nan value for a weather feature list depending on what it originally contains
    :param weather_feature_list: weather feature values to be summed
    :return: averaged weather feature summed (or np.nan)
    """
    if len(weather_feature_list) == 0:
        value = np.nan
    else:
        value = round(sum(weather_feature_list))
    return value


def make_counted_weather_feature(weather_feature_list):
    """
    Function returns the value for the highest occurring elementor np.nan value for a weather feature list depending on what it originally contains
    :param weather_feature_list: weather feature values to be counted
    :return: highest occurring elementor (or np.nan)
    """
    if len(weather_feature_list) == 0:
        value = np.nan
    else:
        weather_condition_counter = collections.Counter(weather_feature_list)
        value = max(weather_condition_counter, key=weather_condition_counter.get)
    return value


def make_condition_from_code(weather_code):
    """
    Function returns the condition (text) from a weather code by checking if the code exists in a dictionary
    :param weather_code: weather code id from Meteostat
    :return: weather condition from Meteostat
    """
    try:
        weather_condition = WEATHER_CODE_CONDITION_DICTIONARY[weather_code]
    except KeyError:
        weather_condition = np.nan
    return weather_condition


def make_weather_feature_mean_and_condition_value(round_air_temp, round_rhumidity, round_precipitation, round_wind_speed, round_weather_code):
    """
    Function returns mean values for air temp, humidity and wind speed. It also returns sum of rainfall and the most frequent weather code
    :param round_air_temp: list of air temperatures
    :param round_rhumidity: list of humidity
    :param round_precipitation: list of precipitation
    :param round_wind_speed: list of wind speed
    :param round_weather_code: list of weather codes
    :return: aggregated values of weather features
    """
    clean_round_air_temp = make_list_without_nan_values(round_air_temp)
    clean_round_rhumidity = make_list_without_nan_values(round_rhumidity)
    clean_round_precipitation = make_list_without_nan_values(round_precipitation)
    clean_round_wind_speed = make_list_without_nan_values(round_wind_speed)
    clean_round_weather_code = make_list_without_nan_values(round_weather_code)
    mean_air_temp = make_mean_weather_feature(clean_round_air_temp)
    mean_rhumidity = make_mean_weather_feature(clean_round_rhumidity)
    sum_precipitation = make_sum_weather_feature(clean_round_precipitation)
    mean_wind_speed = make_mean_weather_feature(clean_round_wind_speed)
    weather_code = make_counted_weather_feature(clean_round_weather_code)
    weather_condition = make_condition_from_code(weather_code)
    return mean_air_temp, mean_rhumidity, sum_precipitation, mean_wind_speed, weather_condition


def make_round_weather_condition_text(mean_air_temp, mean_rhumidity, sum_precipitation, mean_wind_speed, weather_condition):
    """
    Function creates the text pattern used to display the golf rounds weather conditions
    :param mean_air_temp: round average air temperature
    :param mean_rhumidity: round average humidity
    :param sum_precipitation: round total precipitation
    :param mean_wind_speed: round average wind speed
    :param weather_condition: round weather condition
    :return: golf round weather text pattern
    """
    text_weather_dict = {WEATHER_CONDITION_TEXT: weather_condition, TEMPERATURE_TEXT: mean_air_temp, WIND_SPEED_TEXT: mean_wind_speed, HUMIDITY_LEVEL: mean_rhumidity, PRECIPITATION_TEXT: sum_precipitation}
    text = ""
    variables_to_format = list()
    for key, value in text_weather_dict.items():
        try:
            if math.isnan(value) == False:
                text = text + key
                variables_to_format.append(value)
        except TypeError:
            text = text + key
            variables_to_format.append(value)
    weather_text = text.format(*variables_to_format)
    return weather_text


def make_pipeline_weather_data_to_text(weather_data):
    """
    Function pipelines the process required to output the golf rounds weather text from the golf rounds weather dataset
    :param weather_data: DataFrame containing hourly weather conditions
    :return: golf round weather text pattern
    """
    round_air_temp, round_rhumidity, round_precipitation, round_wind_speed, round_weather_code = make_round_weather_condition_lists(weather_data)
    mean_air_temp, mean_rhumidity, sum_precipitation, mean_wind_speed, weather_condition = make_weather_feature_mean_and_condition_value(round_air_temp, round_rhumidity, round_precipitation, round_wind_speed, round_weather_code)
    weather_text = make_round_weather_condition_text(mean_air_temp, mean_rhumidity, sum_precipitation, mean_wind_speed, weather_condition)
    return weather_text, mean_air_temp, mean_rhumidity, sum_precipitation, mean_wind_speed, weather_condition


def make_course_feature_using_course_id(course_id, table, holes="18"):
    """
    Function returns the row for a course feature table based on the id passed. It returns the specified number of holes 18, Front 9 or Back 9 based on the "holes" parameter
    :param course_id: id of course in course table
    :return: list of course features
    """
    insert_command = """SELECT * FROM {}
                WHERE course_id = %s;""".format(table)
    cursor.execute(insert_command, [course_id])
    returned_value = cursor.fetchall()
    feature_list = list(returned_value[0])
    if holes == "Front 9":
        feature_list = feature_list[1:10]
    elif holes == "Back 9":
        feature_list = feature_list[10:]
    else:
        feature_list = feature_list[1:]
    return feature_list


def make_hole_number_list(holes):
    """
    Function creates a list of numbers. Either 1-9, 10-18 holes or 1-18
    :param holes: number of holes the course has
    :return: list of holes where number of holes is either 1-9, 10-18 holes or 1-18
    """
    if holes == "Front 9":
        list_of_holes = list(range(1, 10))
    elif holes == "Back 9":
        list_of_holes = list(range(10, 19))
    elif holes == "18":
        list_of_holes = list(range(1, 19))
    else:
        pass
    return list_of_holes


def make_all_course_feature_lists(course_id, holes="18"):
    """
    Function pipelines all three calls to the three course feature tables
    :param course_id: id of course in course table
    :return: three lists made of distance, par, si
    """
    list_of_holes = make_hole_number_list(holes)
    course_distance_list = make_course_feature_using_course_id(course_id, DISTANCE_TABLE, holes)
    course_par_list = make_course_feature_using_course_id(course_id, PAR_TABLE, holes)
    course_si_list = make_course_feature_using_course_id(course_id, SI_TABLE, holes)
    return list_of_holes, course_distance_list, course_par_list, course_si_list


def make_round_score_card_csv(list_of_holes, course_distance_list, course_si_list, course_par_list):
    """
    Function will generate a .csv containing the course distance/si/par along with blank entries for strokes/putts/fir/gir
    :param course_distance_list: the distances of each hole on the course
    :param course_si_list: the stroke index of each hole on the course
    :param course_par_list: the par of each hole on the course
    :return: to_csv object that can be downloaded
    """
    round_score_card_template_df = pd.DataFrame()
    round_score_card_template_df["Hole"] = list_of_holes
    round_score_card_template_df["Distance"] = course_distance_list
    round_score_card_template_df["Stroke Index"] = course_si_list
    round_score_card_template_df["Par"] = course_par_list
    round_score_card_template_df["Shots"] = ""
    round_score_card_template_df["Putts"] = ""
    round_score_card_template_df["FIR"] = ""
    round_score_card_template_df["GIR"] = ""
    round_score_card_template_df_csv = round_score_card_template_df.to_csv(index=False)
    return round_score_card_template_df_csv


def make_pipeline_round_score_card_csv(course_name, holes="18"):
    """
    Function pipelines the process required to generate the round score card which includes course features such distance/si/par along with blank entries for strokes/putts/fir/gir
    :param course_name: course name
    :return: round score card csv
    """
    course_id = get_id_from_course_name(course_name)
    list_of_holes, course_distance_list, course_par_list, course_si_list = make_all_course_feature_lists(course_id, holes)
    round_score_card_template_df_csv = make_round_score_card_csv(list_of_holes, course_distance_list, course_si_list, course_par_list)
    return round_score_card_template_df_csv


def make_date_time():
    """
    Function makes the current date and time
    :return: current date and time
    """
    date_created = datetime.today().date()
    time_created = datetime.now().time().strftime("%H:%M:%S")
    return date_created, time_created


# Insert round data into round table
def insert_round_in_round_table(course_id, user_id, date_played, tee_time, temperature, humidity, wind_speed, precipitation, weather_condition, holes_played):
    """
    Function inserts table information into the table table
    :param course_id: id of course
    :param user_id: id of user
    :param date_played: date played
    :param tee_time: time tee'd off
    :param temperature: average round temperature
    :param humidity: average round humidity
    :param wind_speed: average round wind speed
    :param precipitation: total round rainfall
    :param weather_condition: round weather condition
    :param holes_played: holes played that round
    :return: date and time round was created
    """
    insert_command = """INSERT INTO round
                  (course_id, user_id, date_played, tee_time, temperature, humidity, wind_speed, precipitation, weather_condition, holes_played, date_created, time_created)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    date_created, time_created = make_date_time()
    data_tuple = (course_id, user_id, date_played, tee_time, temperature, humidity, wind_speed, precipitation, weather_condition, holes_played, date_created, time_created)
    cursor_execute_tuple(insert_command, data_tuple)
    return date_created, time_created


def get_round_id_from_round_features(course_id, user_id, date_played, tee_time, date_created, time_created):
    """
    Function returns the round id for a round based on key features of that round
    :param course_id: id of course
    :param user_id: id of user
    :param date_played: date round was played
    :param tee_time: time tee time was
    :param date_created: date round was created
    :param time_created: time round was created
    :return: round of id
    """
    insert_command = """SELECT round_id FROM round
                    WHERE course_id = %s AND user_id = %s AND date_played = %s AND tee_time = %s AND date_created = %s AND time_created = %s;"""
    cursor.execute(insert_command, [course_id, user_id, date_played, tee_time, date_created, time_created])
    returned_value = cursor.fetchall()
    round_id = returned_value[0][0]
    return round_id


# Insert round features shots/putts/fir/gir
def make_data_tuple_front9(table, round_feature, round_id):
    """
    Function creates the SQL command needed to insert features into a table for the FRONT9
    :param table: name of table in database
    :param round_feature: list of features
    :param round_id: id of round in round table
    :return: insert SQL command, features as tuple
    """
    insert_command = """INSERT INTO {}
                  (round_id, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8, hole9)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(table)
    round_id_list = [round_id]
    round_id_tuple = tuple(round_id_list)
    feature_tuple = tuple(round_feature)
    data_tuple = tuple(round_id_tuple) + tuple(feature_tuple)
    return insert_command, data_tuple


def make_data_tuple_back9(table, round_feature, round_id):
    """
    Function creates the SQL command needed to insert features into a table for the BACK9
    :param table: name of table in database
    :param round_feature: list of features
    :param round_id: id of round in round table
    :return: insert SQL command, features as tuple
    """
    insert_command = """INSERT INTO {}
                  (round_id, hole10, hole11, hole12, hole13, hole14, hole15, hole16, hole17, hole18)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(table)
    round_id_list = [round_id]
    round_id_tuple = tuple(round_id_list)
    feature_tuple = tuple(round_feature)
    data_tuple = tuple(round_id_tuple) + tuple(feature_tuple)
    return insert_command, data_tuple


def make_data_tuple_18holes(table, round_feature, round_id):
    """
    Function creates the SQL command needed to insert features into a table - 18 holes
    :param table: name of table in database
    :param round_feature: list of features
    :param round_id: id of round in round table
    :return: insert SQL command, features as tuple
    """
    insert_command = """INSERT INTO {}
                      (round_id, hole1, hole2, hole3, hole4, hole5, hole6, hole7, hole8, hole9, hole10, hole11, hole12, hole13, hole14, hole15, hole16, hole17, hole18)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(
        table)
    round_id_list = [round_id]
    round_id_tuple = tuple(round_id_list)
    feature_tuple = tuple(round_feature)
    data_tuple = tuple(round_id_tuple) + tuple(feature_tuple)
    return insert_command, data_tuple


def insert_round_score_card_feature_to_table(table, round_id, holes_played, round_feature):
    """
    Function pipelines the process to insert 9/18 hole round feature information (shots, putts, fir, gir) into a database table
    :param table: name of table in database
    :param round_feature: list of features
    :param holes_played: the amount of holes played
    :param round_id: id of round in round table
    :return:
    """
    if holes_played == FRONT9:
        insert_command, data_tuple = make_data_tuple_front9(table, round_feature, round_id)
        cursor_execute_tuple(insert_command, data_tuple)
    elif holes_played == BACK9:
        insert_command, data_tuple = make_data_tuple_back9(table, round_feature, round_id)
        cursor_execute_tuple(insert_command, data_tuple)
    elif holes_played == ALL18:
        insert_command, data_tuple = make_data_tuple_18holes(table, round_feature, round_id)
        cursor_execute_tuple(insert_command, data_tuple)
    return None


def make_round_df_and_insert_round_feature(file_path, holes_played, round_id):
    """
    Function will read a file path (CSV) and insert features to various round_feature tables
    :param file_path: path of file (CSV)
    :param holes_played: number of holes played in round
    :param round_id: id of round
    :return: None
    """
    temp_df = pd.read_csv(file_path)
    round_shots_list = list(temp_df["Shots"])
    round_putts_list = list(temp_df["Putts"])
    round_fir_list = list(temp_df["FIR"])
    round_gir_list = list(temp_df["GIR"])
    insert_round_score_card_feature_to_table(ROUND_SHOTS_TABLE, round_id, holes_played, round_shots_list)
    insert_round_score_card_feature_to_table(ROUND_PUTTS_TABLE, round_id, holes_played, round_putts_list)
    insert_round_score_card_feature_to_table(ROUND_FIR_TABLE, round_id, holes_played, round_fir_list)
    insert_round_score_card_feature_to_table(ROUND_GIR_TABLE, round_id, holes_played, round_gir_list)
    return None


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
    rounds_to_be_used = returned_value[0][1]
    adjustment = returned_value[0][2]
    return rounds_to_be_used, adjustment


def make_logic_round_played_calculation_hcp_index_tuple(rounds_played):
    """
    Function creates the logic requried to return the correct round played tuple from the calculation_handicap_index tuple
    :param rounds_played: number of rounds played
    :return: rounds played, rounds to be used and adjustment
    """
    if rounds_played < 3:
        return (np.nan, np.nan)
    elif rounds_played > 20:
        rounds_played = 20
        return make_sql_rounds_played_calculation_hcp_index(rounds_played)
    else:
        return make_sql_rounds_played_calculation_hcp_index(rounds_played)


def make_sql_rounds_score_differential_df(user_id):
    """
    Function returns a dataframe containing the round features and its score differential
    :param user_id: id of user
    :return: dataframe containing the round features and its score differential
    """
    insert_command = """SELECT round.round_id, round.date_played, c.slope, COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0) AS round_score, c.rating,
                               ( (113 / c.slope) * ((COALESCE(rs.hole1,0) + COALESCE(rs.hole2,0) + COALESCE(rs.hole3,0) +
                            COALESCE(rs.hole4,0) +COALESCE(rs.hole5,0) + COALESCE(rs.hole6,0) + COALESCE(rs.hole7,0) + COALESCE(rs.hole8,0) +
                            COALESCE(rs.hole9,0) + COALESCE(rs.hole10,0) + COALESCE(rs.hole11,0) + COALESCE(rs.hole12,0) + COALESCE(rs.hole13,0) +
                            COALESCE(rs.hole14,0) + COALESCE(rs.hole15,0) + COALESCE(rs.hole16,0) + COALESCE(rs.hole17,0) + COALESCE(rs.hole18,0)) - c.rating) ) AS score_differntial
                        FROM round
                        JOIN course c on round.course_id = c.course_id
                        JOIN round_shots rs on round.round_id = rs.round_id
                        WHERE round.user_id = %(user_id)s
                        ORDER BY round.date_played DESC;"""
    score_differential_df = pd.read_sql_query(insert_command, con=engine, params={"user_id": user_id})
    return score_differential_df


def truncate(number, digits):
    """
    Function truncates a number to a specified digit
    :param number: number to be truncated
    :param digits: number of digits to truncate to
    :return: truncated number
    """
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper


def make_avg_score_diffential(score_differential_df, rounds_to_be_used):
    """
    Function returns the truncated average score differential from the specified number of rounds to be used
    :param score_differential_df: dataframe containing the round features and its score differential
    :param rounds_to_be_used:  rounds that should be used in the HCP calculation
    :return: truncated average score differential
    """
    latest_20_score_differential_df = score_differential_df[:20]
    best_score_differntial_df = latest_20_score_differential_df.sort_values("score_differntial")[:rounds_to_be_used]
    avg_score_diffential = best_score_differntial_df["score_differntial"].mean()
    truncated_avg_score_diffential = truncate(avg_score_diffential, 1)
    return truncated_avg_score_diffential


def make_handicap_index(truncated_avg_score_differential, adjustment):
    """
    Function returns the handicap index by subtracting adjustment from the truncated_avg_score_diffential
    :param truncated_avg_score_differential: score differential
    :param adjustment: number of strokes to be subtracted as per calculated_handicap_index table
    :return: handicap index
    """
    handicap_index = truncated_avg_score_differential - adjustment
    return handicap_index


def pipeline_make_handicap_index(user_id):
    """
    Function pipelines the entire process required to calculate a users handicap index based on rounds played and the World Handicap System
    :param user_id: id of user
    :return:
    """
    rounds_played = make_sql_count_of_rounds_played(user_id)
    rounds_to_be_used, adjustment = make_logic_round_played_calculation_hcp_index_tuple(rounds_played)
    score_differential_df = make_sql_rounds_score_differential_df(user_id)
    truncated_avg_score_differential = make_avg_score_diffential(score_differential_df, rounds_to_be_used)
    handicap_index = make_handicap_index(truncated_avg_score_differential, adjustment)
    return handicap_index


def insert_hcp_into_dashboard_user_hcp_table(user_id, handicap_index):
    """
    Function inserts user_id, hcp index and date created in the hcp table
    :param user_id: id of user
    :param handicap_index: handicap index
    :return: None
    """
    insert_command = """INSERT INTO dashboard_user_handicap
                      (user_id, handicap_index, date_created, time_created)
                      VALUES (%s, %s, %s, %s);"""
    date_created, time_created = make_date_time()
    data_tuple = (user_id, handicap_index, date_created, time_created)
    cursor_execute_tuple(insert_command, data_tuple)
    return None


################################ STREAMLIT #######################################


# Connect to DB
con, cursor = connect_to_postgres_database(USER, PASSWORD, DATABASE, HOST, port="5432")
engine = create_engine("postgresql+psycopg2://" + USER + ":" + PASSWORD + "@" + HOST + "/" + DATABASE)


def app():
    """
    Function to render the add_golf_round.py page via the app.py file
    """
    st.subheader("Upload a round 🏌🏽‍♂️")
    try:
        # Session State User ID
        user_id = st.session_state["user_id"]
        user_id is not None
        # user_id = 1
        # Adding round instructions
        with st.expander("Click to learn how to add a round"):
            st.write("1. Populate all fields in this below")
            st.write("2. Click *'Add round and download round score card template'*")
            st.write("3. Fill in the downloaded score card with your round shots. If available "
                     "fill in Putts/FIR/GIR")
            st.write("5. When filling in FIR/GIR, use a *1* to represent a yes (success) and a "
                     "*0* to represent no (failure). When filling in FIR for par 3's (FIR not "
                     "applicable) please put a *0* (zero)")
            st.write("6. Upload the filled out score card using the file uploader")
            st.write("")
            st.write("*Can't find your course? Add via the App Navigation menu and then continue on "
                     "adding your round!")
        st.write("")
        # Select course name
        sorted_feature_list = make_alphabetical_course_name_list()
        course_name_select = st.selectbox(
            'Where did you play your round?',
            sorted_feature_list)
        course_id = get_id_from_course_name(course_name_select)
        st.write("")
        # Select round date
        date_input = st.date_input(
            "When did you play your round?",
            value=datetime.today(), max_value=datetime.today())
        st.write("")
        # Select tee time
        time_input = st.number_input('What hour did you start your round?', min_value=0, max_value=23)
        st.write("")
        # Select holes played
        hole_input = st.selectbox('How many holes did you play?', (18, 9))
        if hole_input == 9:
            front_back_9 = st.selectbox('Did you play the Front or Back 9?', ("Front 9",
                                                                              "Back 9"))
            if front_back_9 == "Front 9":
                holes_played = "Front 9"
            elif front_back_9 == "Back 9":
                holes_played = "Back 9"
            else:
                pass
        elif hole_input == 18:
            holes_played = "18"
        else:
            st.error("Please select either 18 or 9")
        # Generate score card template
        if course_name_select != "":
            round_score_card_template_df_csv = make_pipeline_round_score_card_csv(course_name_select, holes_played)
        if course_name_select != "" and time_input > 0:
            # Add round and download score card template button
            if st.download_button(
                    label="Add round and download round score card template",
                    data=round_score_card_template_df_csv,
                    file_name="golf_round_score_card_template.csv"):
                # Get weather info
                start_time = time(time_input, 0)
                end_time = time(time_input + 4, 59)
                start_datetime = datetime.combine(date_input, start_time)
                end_datetime = datetime.combine(date_input, end_time)
                city, country = get_city_country_from_course_id(course_id)
                data = make_pipeline_city_historical_hourly_weather_df(city, country,
                                                                       start_datetime, end_datetime)
                weather_text, mean_air_temp, mean_rhumidity, sum_precipitation, mean_wind_speed, weather_condition = make_pipeline_weather_data_to_text(data)
                # Insert round in database
                try:
                    date_created, time_created = insert_round_in_round_table(course_id, user_id,
                                                                             date_input,
                                                                             time_input,
                                                                             mean_air_temp,
                                                                             mean_rhumidity, mean_wind_speed, sum_precipitation, weather_condition, hole_input)
                    round_id = get_round_id_from_round_features(course_id, user_id, date_input, time_input,
                                                                date_created, time_created)
                    # Initialize round id session state
                    if "round_id" not in st.session_state:
                        st.session_state['round_id'] = round_id
                except:
                    pass
            # File uploader
            uploaded_file = st.file_uploader("Upload round score card")
            if uploaded_file is not None:
                try:
                    # Insert round to db
                    make_round_df_and_insert_round_feature(uploaded_file, holes_played,
                                                           st.session_state['round_id'])
                    st.success("Successfully added your round")
                    # Calculate and insert HCP
                    handicap_index = pipeline_make_handicap_index(user_id)
                    if math.isnan(handicap_index):
                        pass
                    else:
                        insert_hcp_into_dashboard_user_hcp_table(user_id, handicap_index)
                except:
                    st.error("We could not upload your file. Please make sure you are uploading "
                             "the *round score card template* downloaded above!")
    except KeyError:
            st.warning("You must login before accessing this page. Please authenticate via the login menu.")
    return None

