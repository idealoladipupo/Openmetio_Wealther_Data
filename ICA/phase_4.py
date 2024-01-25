# Author: Idowu Oladipupo
# Student ID: D3180615


# importing custom module to handle database connection
from custom_module import db_connection_module as con 

# Import other dependencies
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import timedelta, datetime

def get_weather_data(lat, long, start_date, end_date):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
            "latitude": lat,
            "longitude": long,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(2).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()

    return daily_temperature_2m_min, daily_temperature_2m_mean, daily_temperature_2m_max, daily_precipitation_sum

    '''
    daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s"),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
    )}
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum

    daily_dataframe = pd.DataFrame(data = daily_data)
    print(daily_dataframe)
    '''

def db_query(connection, query):
    cursor = connection.cursor()
    results = cursor.execute(query)
    return results

def get_gps(connection, city_id):
    query = f"select  longitude, latitude from cities where id = {city_id}"
    result = db_query(connection, query).fetchone()
    return result[0], result[1]
    

def write_data(connection, query, data):
    cursor = connection.cursor()
    results = cursor.execute(query, data)
    connection.commit()

def check_date_exists(connection, city_id, date):
    query = f"select count(*) from daily_weather_entries where city_id = {city_id} AND date = '{date}'"
    result = db_query(connection, query).fetchone()
    return result[0]

def get_cities(connection):
    query = f"select id, name from cities order by id"
    result = db_query(connection, query)
    ids = []
    names = []

    for city in result:
        ids.append(int(city[0]))
        names.append(city[1])
        
    return ids, names

def main(connection):
    
    if connection:
        
        ids, names = get_cities(connection)
        for index in range(len(ids)):
            print(f"ID {ids[index]}  City Name {names[index]}")
            
        print("********************************************************************************")

        
        city_id = (input(f"Select City ID from ( 1 - {len(ids)} ): "))
        # Convert city_id input gotten from user to integer to check if its a number input
        try:
            city_id = int(city_id) 

        except ValueError:
            print(f"Entry must be a number and cannot be empty, Please select City ID from ( 1 - {len(ids)} ) ")
            return

        if city_id not in ids:
            print(f"That is not a city ID, Please select City ID from ( 1 - {len(ids)} ) ") 
        else: 
            start_date = input("Enter Start Date (YYYY-MM-DD): ")
            end_date = input("Enter End Date (YYYY-MM-DD): ")
            try:
            # Check if date is valid and stringify it, else return an error for date format 
                start_date = str(datetime.strptime(start_date, '%Y-%m-%d').date())
                end_date = str(datetime.strptime(start_date, '%Y-%m-%d').date())
            except ValueError:
                print("Start and End date must be in the format 'YYYY-MM-DD'.")
                return

            long, lat = get_gps(connection, city_id)
            print(long, lat)

            min_temp, mean_temp, max_temp, precipitation = get_weather_data(lat, long, start_date, end_date)
            for i in range(len(min_temp)):
                    print(f"{i:6} {min_temp[i]:6.2f}  {mean_temp[i]:6.2f}  {max_temp[i]:6.2f}   {precipitation[i]:6.2f} ")


            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            print(start_date.strftime("%Y-%m-%d"), start_date)

            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            print(end_date.strftime("%Y-%m-%d"), end_date)

            index = 0
            while start_date <= end_date:

                result = check_date_exists(connection, city_id, start_date.strftime("%Y-%m-%d"))

                if result > 0:
                    print(f"{start_date.strftime('%Y-%m-%d')} {city_id}   Already in DB")
                else:
                    print(f"{start_date.strftime('%Y-%m-%d')} {index:6} {min_temp[index]:6.2f}  {mean_temp[index]:6.2f}  {max_temp[index]:6.2f}   {precipitation[index]:6.2f}  {city_id}" )

                    query = "INSERT into daily_weather_entries (date, min_temp, max_temp, mean_temp, precipitation, city_id) VALUES (?, ?, ?, ?, ?, ?)"
                    data = (start_date.strftime("%Y-%m-%d"), f"{min_temp[index]:.2f}", f"{max_temp[index]:.2f}", f"{mean_temp[index]:.2f}", f"{precipitation[index]:.2f}", str(city_id ))

                    write_data(connection, query, data)

                start_date += timedelta(days=1)
                index += 1     
    pass  

if __name__ == "__main__":
    # ******************************************************************************#
    # Database path
    db_path='db\\CIS4044-N-SDI-OPENMETEO-PARTIAL.db'

    # Initiate database connection, using custom database connection module
    connection = con.connect_to_database(db_path)

    # Call main function with the database conection argument 
    main(connection)
    # ******************************************************************************#
    pass

    

