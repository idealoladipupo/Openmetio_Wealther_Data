# Author: Idowu Oladipupo
# Student ID: D3180615

# importing custom module to handle database connection
from custom_module import db_connection_module as con

# Import other dependencies
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import pandas as pd


# Function to qeury and select Avergae temperature and preciptation between dates
def get_mean_temp_and_precipitation_within_dates(
    connection, item_id, item_type, start_date, end_date
):
    try:
        cursor = connection.cursor()
        # Query to select by city
        if item_type == "1":
            query = """
                SELECT AVG(daily_weather_entries.mean_temp) AS mean_temp,
                AVG(daily_weather_entries.precipitation) AS mean_precipitation
                FROM daily_weather_entries
                WHERE daily_weather_entries.city_id = ?
                AND daily_weather_entries.date BETWEEN ? AND ?
            """
        # Query to select by country
        elif item_type == "2":
            query = """
                SELECT AVG(daily_weather_entries.mean_temp) AS mean_temp,
                AVG(daily_weather_entries.precipitation) AS mean_precipitation
                FROM daily_weather_entries
                JOIN cities ON daily_weather_entries.city_id = cities.id
                WHERE cities.country_id = ?
                AND daily_weather_entries.date BETWEEN ? AND ?
            """
        else:
            return None, None

        # Execute query
        cursor.execute(query, (item_id, start_date, end_date))
        # Fetch the result
        result = cursor.fetchone()
        # Compute result and asign mean_temp and mean_precipitation values
        mean_temp = result["mean_temp"] if result["mean_temp"] is not None else 0.0
        mean_precipitation = (
            result["mean_precipitation"]
            if result["mean_precipitation"] is not None
            else 0.0
        )

        return mean_temp, mean_precipitation

    except Exception as ex:
        print(f"Error: {ex}")
        return None, None


# Function to query min and max temperatures between dates
def get_weather_data_for_city_and_date_range(connection, city_id, start_date, end_date):
    try:
        cursor = connection.cursor()
        query = """
            SELECT date, min_temp, max_temp
            FROM daily_weather_entries
            WHERE city_id = ? AND date BETWEEN ? AND ?
            ORDER BY date
        """

        cursor.execute(query, (city_id, start_date, end_date))
        weather_data = cursor.fetchall()

        return weather_data

    except Exception as ex:
        raise ex


# Function to get country name based on country ID
def get_country_name_by_countryid(connection, country_id):
    try:
        cursor = connection.cursor()
        query = "SELECT name FROM countries WHERE id = ?;"
        cursor.execute(query, (country_id,))
        result = cursor.fetchone()
        return result["name"] if result else f"Unknown country ({country_id})"

    except sqlite3.Error as ex:
        print(f"Error querying data: {ex}")
        return f"Country ID {country_id}"


# Function to get country name based on city ID
def get_country_name_by_cityid(connection, city_id):
    try:
        cursor = connection.cursor()
        query = """
            SELECT countries.name
            FROM cities
            JOIN countries ON cities.country_id = countries.id
            WHERE cities.country_id = ?;
        """
        cursor.execute(query, (city_id,))
        result = cursor.fetchone()
        return result["name"] if result else f"Unknown country ({city_id})"

    except sqlite3.Error as ex:
        print(f"Error querying data: {ex}")
        return f"Country ID {city_id}"


# Function to get all available city IDs
def get_all_city_ids(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT DISTINCT city_id FROM daily_weather_entries")
    city_ids = []
    for entry in cursor.fetchall():
        city_ids.append(entry["city_id"])
    return city_ids


# Function to get city name based on city ID
def get_city_name(connection, city_id):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM cities WHERE id = ?", (city_id,))
        result = cursor.fetchone()
        return result["name"] if result else f"Unknown City ({city_id})"

    except sqlite3.Error as e:
        print(f"Error querying data: {e}")
        return f"City ID {city_id}"


# Function to get all country IDs from city_id rows contained in daily weather entries
def get_all_country_ids(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT city_id FROM daily_weather_entries")
        city_ids = []
        for entry in cursor.fetchall():
            city_ids.append(entry["city_id"])

        country_ids = []
        for city_id in city_ids:
            cursor.execute("SELECT country_id FROM cities WHERE id = ?", (city_id,))
            result = cursor.fetchone()
            if result:
                country_ids.append(result["country_id"])

        return list(set(country_ids))

    except Exception as ex:
        print(f"Error: {ex}")
        return None


# Function to calculate Average seven day precipitation for a city or set of cities from a given date
def bar_chart_7_day_prep(connection):
    try:
        # Get the list of available cities
        available_cities = get_all_city_ids(connection)

        # Get user input for cities
        print("Available Cities:")
        for city_id in available_cities:
            print(f"ID: {city_id}: {get_city_name(connection, city_id)}")

        user_input = input(
            "Enter city IDs separated by commas (or type 'all' for all cities): "
        )
        if user_input.lower() == "all":
            city_ids = available_cities
        else:
            city_ids = []
            for city_id in user_input.split(","):
                stripped_id = city_id.strip()
                int_id = int(stripped_id)
                city_ids.append(int_id)

        # Get user input for start and end date
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = datetime.strptime(start_date, "%Y-%m-%d").date() + timedelta(days=7)

        cursor = connection.cursor()
        query = (
            "SELECT city_id, AVG(precipitation) AS avg_precipitation "
            "FROM daily_weather_entries "
            "WHERE city_id IN ({}) AND date BETWEEN ? AND ? "
            "GROUP BY city_id"
        ).format(", ".join(map(str, city_ids)))

        cursor.execute(query, (start_date, end_date))
        city_precipitation_data = cursor.fetchall()

        city_names = []
        for city_id in city_ids:
            name = get_city_name(connection, city_id)
            city_names.append(name)

        avg_precipitation = []
        for entry in city_precipitation_data:
            avg_precipitation.append(entry["avg_precipitation"])

        plt.bar(city_names, avg_precipitation, color="blue")
        plt.xlabel("City")
        plt.ylabel("Average 7 Day Precipitation (mm)")
        plt.title(f"Average Precipitation for Period ({start_date} to {end_date})")
        plt.show()

    except Exception as ex:
        print(f"Error: {ex}")


# Function to calculate Average precipitation for a city or set of cities between two dates
def bar_chart_prep_btw_dates(connection):
    try:
        # Get the list of available cities
        available_cities = get_all_city_ids(connection)

        # Get user input for cities
        print("Available Cities:")
        for city_id in available_cities:
            print(f"{city_id}: {get_city_name(connection, city_id)}")

        user_input = input(
            "Enter city IDs separated by commas (or type 'all' for all cities): "
        )
        if user_input.lower() == "all":
            city_ids = available_cities
        else:
            city_ids = [int(city_id.strip()) for city_id in user_input.split(",")]

        # Get user input for start and end date
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")

        cursor = connection.cursor()
        query = (
            "SELECT city_id, AVG(precipitation) AS avg_precipitation "
            "FROM daily_weather_entries "
            "WHERE city_id IN ({}) AND date BETWEEN ? AND ? "
            "GROUP BY city_id"
        ).format(", ".join(map(str, city_ids)))

        cursor.execute(query, (start_date, end_date))
        city_precipitation_data = cursor.fetchall()

        city_names = []
        for city_id in city_ids:
            city_names.append(get_city_name(connection, city_id))

        avg_precipitation = [
            entry["avg_precipitation"] for entry in city_precipitation_data
        ]

        plt.bar(city_names, avg_precipitation, color="blue")
        plt.xlabel("City")
        plt.ylabel("Average Precipitation (mm)")
        plt.title(f"Average Precipitation for Period ({start_date} to {end_date})")
        plt.show()

    except Exception as ex:
        print(f"Error: {ex}")


# Function to calculate Average Yearly precipitation by country
def bar_chart_annual_prep_by_countries(connection):
    try:
        # Get the list of available country IDs
        available_country_ids = get_all_country_ids(connection)

        # Get user input for country ID
        print("Available Country IDs:")
        for country_id in available_country_ids:
            print(
                f"{country_id}: {get_country_name_by_countryid(connection, country_id)}"
            )

        user_input = input(
            "Enter country IDs separated by commas (or type 'all' for all countries): "
        )
        if user_input.lower() == "all":
            country_ids = available_country_ids
        else:
            country_ids = []
            for country_id in user_input.split(","):
                country_ids.append(int(country_id.strip()))

        # Get user input for desired year
        year = input("Enter the year (YYYY): ")

        cursor = connection.cursor()
        query = """
            SELECT cities.country_id, AVG(daily_weather_entries.precipitation) AS avg_precipitation
            FROM daily_weather_entries
            JOIN cities ON daily_weather_entries.city_id = cities.id
            WHERE cities.country_id IN ({})
            AND strftime('%Y', daily_weather_entries.date) = ?
            GROUP BY cities.country_id
        """.format(
            ", ".join(map(str, country_ids))
        )

        cursor.execute(query, (year,))
        country_precipitation_data = cursor.fetchall()

        country_names = [
            get_country_name_by_countryid(connection, country_id)
            for country_id in set(c["country_id"] for c in country_precipitation_data)
        ]
        avg_precipitation = [
            entry["avg_precipitation"] for entry in country_precipitation_data
        ]

        plt.bar(country_names, avg_precipitation, color="blue")
        plt.xlabel("Country")
        plt.ylabel("Average Annual Precipitation (mm)")
        plt.title(f"Average Annual Precipitation for Year {year}")
        plt.show()

    except Exception as ex:
        print(f"Error: {ex}")


# Function to generate group plot for Annual Min,Max, Mean temperature and precipitation by country or city
def grp_chart_temp_and_prep(connection):
    try:
        # Get input to generate the chart for cities or countries
        print("Choose operation to Generate Grouped Bar chart:")
        print("1. City")
        print("2. Country")
        user_input_type = input("Enter 1 for city or 2 for country: ")

        if user_input_type == "1":
            # Get the list of available city IDs
            available_ids = get_all_city_ids(connection)
        elif user_input_type == "2":
            # Get all available country IDs
            available_ids = get_all_country_ids(connection)
        else:
            print("Invalid input. Please enter 1 for city or 2 for country.")
            return

        # Display available cities or countries
        print(f"Available {'' if user_input_type == '1' else 'Country'} IDs:")
        for item_id in available_ids:
            print(
                f"{item_id}: {get_city_name(connection, item_id) if user_input_type == '1' else get_country_name_by_countryid(connection, item_id)}"
            )

        # Get user input for city id or all
        user_input = input(
            f"Enter {'City' if user_input_type == '1' else 'Country'}ID: (type 'all' for all): "
        )

        selected_ids = []
        if user_input.lower() == "all":
            selected_ids = available_ids
        else:
            selected_ids = [int(item_id.strip()) for item_id in user_input.split(",")]

        # Get user input for start and end date
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")

        cursor = connection.cursor()
        query = """
            SELECT city_id, 
            AVG(min_temp) AS avg_min_temp,
            AVG(max_temp) AS avg_max_temp,
            AVG(mean_temp) AS avg_mean_temp,
            AVG(precipitation) AS avg_precipitation
            FROM daily_weather_entries
            WHERE city_id IN ({}) AND date BETWEEN ? AND ?
            GROUP BY city_id
        """.format(
            ", ".join(map(str, selected_ids))
        )

        cursor.execute(query, (start_date, end_date))
        city_weather_data = cursor.fetchall()
        # Extract city names
        city_names = []
        for city_id in set(entry["city_id"] for entry in city_weather_data):
            if user_input_type == "1":
                city_names.append(get_city_name(connection, city_id))
            else:
                city_names.append(get_country_name_by_cityid(connection, city_id))

        avg_min_temp = [entry["avg_min_temp"] for entry in city_weather_data]
        avg_max_temp = [entry["avg_max_temp"] for entry in city_weather_data]
        avg_mean_temp = [entry["avg_mean_temp"] for entry in city_weather_data]
        avg_precipitation = [entry["avg_precipitation"] for entry in city_weather_data]

        # Plot grouped bar chart
        bar_width = 0.2
        index = range(len(city_names))

        plt.bar(index, avg_min_temp, width=bar_width, label="Min Temp")
        plt.bar(
            [i + bar_width for i in index],
            avg_max_temp,
            width=bar_width,
            label="Max Temp",
        )
        plt.bar(
            [i + 2 * bar_width for i in index],
            avg_mean_temp,
            width=bar_width,
            label="Mean Temp",
        )
        plt.bar(
            [i + 3 * bar_width for i in index],
            avg_precipitation,
            width=bar_width,
            label="Precipitation",
        )

        plt.xlabel(f'{"" if user_input_type == "1" else "Country"}')
        plt.ylabel("Average Values")
        plt.title(
            f'Average Weather Data for Selected {"" if user_input_type == "1" else "Country"} ({start_date} to {end_date})'
        )
        plt.xticks([i + 1.5 * bar_width for i in index], city_names)
        plt.legend()
        plt.show()

    except Exception as ex:
        print(f"Error: {ex}")


# Function to generate Multiline plot for Min, Max temperatures for a given month for a specific city
def mtl_chart_min_max_temp_city(connection):
    try:
        # Get the list of available city IDs
        available_ids = get_all_city_ids(connection)

        # Display available cities
        print("Available City IDs:")
        for city_id in available_ids:
            print(f"{city_id}: {get_city_name(connection, city_id)}")

        # Get user input for city ID
        user_input_city = input("Enter City ID: ")
        city_id = int(user_input_city)

        # Get user input for the start date and end date
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")

        # Get weather data for the selected city and date range
        weather_data = get_weather_data_for_city_and_date_range(
            connection, city_id, start_date, end_date
        )

        if not weather_data:
            print(
                f"No data available for {get_city_name(connection, city_id)} in the specified date range."
            )
            return

        # Extract data for plotting
        dates = [entry["date"] for entry in weather_data]
        parameters = ["min_temp", "max_temp"]

        # Plot multiline chart for each parameter with different colors
        colors = ["blue", "red"]
        for i, parameter in enumerate(parameters):
            values = [entry[parameter] for entry in weather_data]
            plt.plot(
                dates,
                values,
                label=f"{parameter.capitalize()} Temperature",
                marker="o",
                color=colors[i],
            )

        # Add horizontal lines for minimum and maximum temperatures
        min_temp_horizontal_line = min(weather_data, key=lambda x: x["min_temp"])[
            "min_temp"
        ]
        max_temp_horizontal_line = max(weather_data, key=lambda x: x["max_temp"])[
            "max_temp"
        ]
        plt.axhline(
            y=min_temp_horizontal_line,
            color="green",
            linestyle="--",
            label="Min Temp (Horizontal Line)",
        )
        plt.axhline(
            y=max_temp_horizontal_line,
            color="orange",
            linestyle="--",
            label="Max Temp (Horizontal Line)",
        )

        plt.xlabel("Date")
        plt.ylabel("Temperature (°C)")
        plt.title(
            f"Min and Max Temperature for {get_city_name(connection, city_id)} for the period ({start_date} to {end_date})"
        )
        plt.xticks(rotation=45)
        plt.legend()
        plt.show()

    except Exception as ex:
        print(f"Error: {ex}")


# Function to generate scatter plot for Average temperature against Average precipitation for town or cities
def sct_chart_annual_temp_by_prep(connection):
    try:
        # Get user input to generate the scatter plot for cities or countries
        print("Choose operation:")
        print("1. City")
        print("2. Country")
        user_input_type = input("Enter 1 for city or 2 for country: ")

        if user_input_type == "1":
            # Get list of all available city IDs
            available_ids = get_all_city_ids(connection)
        elif user_input_type == "2":
            # Get list of all available country IDs
            available_ids = get_all_country_ids(connection)
        else:
            print("Invalid input. Please enter 1 for city or 2 for country.")
            return

        # Display available cities or countries
        print(f"Available {'' if user_input_type == '1' else 'Country'} IDs:")
        for item_id in available_ids:
            print(
                f"{item_id}: {get_city_name(connection, item_id) if user_input_type == '1' else get_country_name_by_countryid(connection, item_id)}"
            )

        # Get user input for country choice
        user_input = input(
            f"Enter {'' if user_input_type == '1' else 'Country'} ID (type 'all' for all): "
        )

        selected_ids = []
        if user_input.lower() == "all":
            selected_ids = available_ids
        else:
            selected_ids = [int(item_id.strip()) for item_id in user_input.split(",")]

        # Get user input for start date and end date
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")

        # Plot scatter plot for mean temperature vs mean precipitation
        for item_id in selected_ids:
            (
                mean_temp_data,
                mean_precipitation_data,
            ) = get_mean_temp_and_precipitation_within_dates(
                connection, item_id, user_input_type, start_date, end_date
            )
            label = f"{get_city_name(connection, item_id) if user_input_type == '1' else get_country_name_by_countryid(connection, item_id)}"
            plt.scatter(
                mean_temp_data, mean_precipitation_data, label=label, marker="o"
            )

        plt.xlabel("Mean Temperature (°C)")
        plt.ylabel("Mean Precipitation (mm)")
        plt.title(f"Scatter Plot for Mean Temperature vs Mean Precipitation")
        plt.legend()
        plt.show()

    except Exception as ex:
        print(f"Error: {ex}")


def main(connection):
    if connection:
        # Call Functions
        # Please remove comments to test each function individually
        bar_chart_7_day_prep(connection)
        # bar_chart_prep_btw_dates(connection)
        # bar_chart_annual_prep_by_countries(connection)
        # grp_chart_temp_and_prep(connection)
        # mtl_chart_min_max_temp_city(connection)
        # sct_chart_annual_temp_by_prep(connection)
        connection.close()


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
