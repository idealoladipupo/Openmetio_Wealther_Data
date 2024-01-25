# Author: Idowu Oladipupo
# Student ID: D3180615

# importing custom module to handle database connection
from custom_module import db_connection_module as con

# Import datetime module
from datetime import datetime, timedelta, date


# Phase 1 - Starter
#
# Note: Display all real/float numbers to 2 decimal places.

"""
Satisfactory
"""


def select_all_countries(connection):
    # Queries the database and selects all the countries
    # stored in the countries table of the database.
    # The returned results are then printed to the
    # console.
    try:
        # Define the query
        query = "SELECT * from [countries]"

        # Get a cursor object from the database connection
        # that will be used to execute database query.
        cursor = connection.cursor()

        # Execute the query via the cursor object.
        results = cursor.execute(query)

        # Iterate over the results and display the results.
        for row in results:
            print(
                f"Country Id: {row['id']} -- Country Name: {row['name']} -- Country Timezone: {row['timezone']}"
            )

    except sqlite3.OperationalError as ex:
        print(ex)


def select_all_cities(connection):
    # TODO: Implement this function
    try:
        # Query to select all cities from cities table
        query = "SELECT * FROM [cities]"
        cursor = connection.cursor()
        # Execute query
        results = cursor.execute(query)

        # Compute result and print to terminal else print error message
        for row in results:
            print(
                f"City Id: {row['id']} -- City Name: {row['name']} -- City Longitude: {row['longitude']} Latitude: {row['latitude']}"
            )

    except sqlite3.OperationalError as ex:
        print(ex)
    pass


"""
Good
"""


def average_annual_temperature(connection, city_id, year):
    # TODO: Implement this function
    try:
        # Convert city_id input gotten from user to integer to check if its a number input
        city_id = int(city_id)
        year = str(year)

    except ValueError:
        print("City ID and year must be integers.")
        return

    try:
        cursor = connection.cursor()

        # Query to retrieve temperature data for the specified city and year
        query = (
            "SELECT AVG(mean_temp) AS avg_temp "
            "FROM daily_weather_entries "
            "WHERE city_id = ? AND strftime('%Y', date) = ?"
        )
        # Execute query
        cursor.execute(query, (city_id, year))

        # Fetch the result
        result = cursor.fetchone()

        # Compute result and print to terminal else print error message
        if result is not None and result["avg_temp"] is not None:
            print(
                f"The average annual temperature for City ID {city_id} in {year} is: {result['avg_temp']:.2f} degrees Celsius."
            )
        else:
            print(f"No data available for City ID {city_id} in {year}.")

    except sqlite3.OperationalError as ex:
        print(ex)
    pass


def average_seven_day_precipitation(connection, city_id, start_date):
    # TODO: Implement this function
    try:
        # Convert city_id input gotten from user to integer to check if its a number input
        city_id = int(city_id)

        # Convert start_date to a datetime object
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        print(
            "City ID must be an integer, and start date must be in the format 'YYYY-MM-DD'."
        )
        return
    try:
        # Set the row factory to access columns by name
        cursor = connection.cursor()

        city_query = "SELECT name FROM cities WHERE id = ?"
        cursor.execute(city_query, (city_id,))
        city_result = cursor.fetchone()

        # Assign city name to city_name variable
        if city_result:
            city_name = city_result["name"]
        else:
            city_name = "Records not found"

        # Calculate the end date by adding 7 days to start date
        end_date = start_date + timedelta(days=7)

        # Query to retrieve precipitation data for the specified city and date range
        query = (
            "SELECT AVG(precipitation) AS avg_precipitation "
            "FROM daily_weather_entries "
            "WHERE city_id = ? AND date BETWEEN ? AND ?"
        )
        # Execute query
        cursor.execute(query, (city_id, start_date, end_date))

        # Fetch the result
        result = cursor.fetchone()

        # Compute result and print to terminal else print error message
        if result is not None and result["avg_precipitation"] is not None:
            print(
                f"The average seven-day precipitation for \nCity ID: {city_id}, City Name: {city_name} starting from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} is: {result['avg_precipitation']:.2f} mm."
            )
        else:
            print(
                f"No data available for City ID {city_id} -- City name {city_name} starting from {start_date.strftime('%Y-%m-%d')}."
            )

    except sqlite3.OperationalError as ex:
        print(ex)
    pass


"""
Very good
"""


def average_mean_temp_by_city(connection, date_from, date_to):
    # TODO: Implement this function
    try:
        # Convert date_from and date_to to datetime objects
        date_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        date_to = datetime.strptime(date_to, "%Y-%m-%d").date()
    except ValueError:
        print("Date format should be 'YYYY-MM-DD'.")
        return
    try:
        # Set the row factory to access columns by name
        cursor = connection.cursor()

        # Query to retrieve the average mean temperature by city and date range
        query = (
            "SELECT city_id, AVG(mean_temp) AS avg_mean_temp "
            "FROM daily_weather_entries "
            "WHERE date BETWEEN ? AND ? "
            "GROUP BY city_id"
        )
        # Execute query
        cursor.execute(query, (date_from, date_to))

        # Fetch the result
        results = cursor.fetchall()

        # Compute result and print to terminal else print error message

        if results:
            print("Average mean temperature by city:")
            for result in results:
                city_id = result["city_id"]
                avg_mean_temp = result["avg_mean_temp"]
                print(f"City ID {city_id}: {avg_mean_temp:.2f} degrees Celsius")
        else:
            print(f"No data available for the specified date range.")
    except sqlite3.OperationalError as ex:
        print(ex)
    pass


def average_annual_precipitation_by_country(connection, year):
    # TODO: Implement this function

    try:
        # Check if year format is correct
        if not (len(year) == 4 and year.isdigit()):
            raise ValueError("Year should be in 'YYYY' format.")
        year = int(year)
    except ValueError as e:
        print(f"Error: {e}")
        return
    try:
        # Set the row factory to access columns by name
        cursor = connection.cursor()

        # Query to retrieve the average annual precipitation by country and year
        query = (
            "SELECT cities.country_id, AVG(daily_weather_entries.precipitation) AS avg_annual_precipitation "
            "FROM daily_weather_entries "
            "JOIN cities ON daily_weather_entries.city_id = cities.id "
            "WHERE strftime('%Y', daily_weather_entries.date) = ? "
            "GROUP BY cities.country_id; "
        )
        # Execute query
        cursor.execute(query, (str(year),))

        # Fetch  result
        results = cursor.fetchall()

        # Compute result and print to terminal else print error message
        if results:
            for result in results:
                country_id = result["country_id"]
                avg_annual_precipitation = result["avg_annual_precipitation"]
                print(f"Country ID {country_id}: {avg_annual_precipitation:.2f} mm")
        else:
            print(f"No data available for the specified year {year}.")

    except sqlite3.OperationalError as ex:
        print(ex)
    pass


"""
Excellent

You have gone beyond the basic requirements for this aspect.

"""


def main(connection):
    if connection:
        # print the results to the terminal.
        print(f"\nSELECT ALL COUNTRIES")
        select_all_countries(connection)

        print(f"\nSELECT ALL CITIES")
        select_all_cities(connection)

        print(f"\nAVERAGE ANNUAL TEMPERATURE")
        average_annual_temperature(connection, 3, 2021)

        print(f"\nAVERAGE SEVEN DAY PRECIPITATION")
        average_seven_day_precipitation(connection, 4, "2021-12-01")

        print(f"\nAVERAGE MEAN TEMP BY CITY")
        average_mean_temp_by_city(connection, "2021-11-03", "2021-12-08")

        print(f"\nAVERAGE ANNUAL PRECIPITATION BY COUNTRY")
        average_annual_precipitation_by_country(connection, "2022")

    pass


if __name__ == "__main__":
    # ******************************************************************************#
    # Database path
    db_path = "db\\CIS4044-N-SDI-OPENMETEO-PARTIAL.db"

    # Initiate database connection, using custom database connection module
    connection = con.connect_to_database(db_path)

    # Call main function with the database conection argument
    main(connection)
    # ******************************************************************************#
    pass
