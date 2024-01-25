# Author: Idowu Oladipupo
# Student ID: D3180615

# importing custom module to handle database connection
from custom_module import db_connection_module as con 

# Import other modules to ease UI operations
import phase_1 as phase1 
import phase_2 as phase2 
import phase_4 as phase4 


class UserInterface: 
    def __init__(self, connection):
        self.connection = connection
    #Function to showcase options 
    def display_menu(self):
        print("********************************************************************************")
        print("===== Weather Data Analysis App Menu =====")
        print("\n===== A. Basic Operations")
        print("1. Print all Countries ID and Name Data contained in database")
        print("2. Print all City ID and Name Data contained in database")
        print("3. Generate Average Annual Temperature for specific city for a given year")
        print("4. Generate Average 7 day precipitation for a desired city from a specific date")
        print("5. Generate Average mean temperature for all cities between two dates")
        print("6. Generate Average Annual precipitation for all countries for a specific year")
        print("\n===== B. Visualization Operations( Charts )")
        print("7. Plot Bar Chart for Average seven day precipitation for cities from a specified date")
        print("8. Plot Bar Chart for Average precipitation between specific dates")
        print("9. Plot Bar Chart for Average Yearly precipitation for all countries")
        print("10. Plot Group Bar Chart showing Min/Max temperatures and precipitation by Month for cities/country")
        print("11.Multiline Chart for Daily Min and Max Temperature")
        print("12. Scatter Plot for Average Temperature vs Average Precipitation")
        print("\n===== C. Database Operations")
        print("13. Update database from Meteo Api for specific City between 2 Dates") 
        print("0. Quit")
        print("********************************************************************************")

    #Functions to handle user choice
    # Phase 1 -- A. Basic Operations starts here
    def get_all_countries_menu(self):
        phase1.select_all_countries(self.connection)

    def get_all_cities_menu(self):
        phase1.select_all_cities(self.connection)

    def get_average_annual_temperature_menu(self):
        city_id = input(f"Enter desire City ID: ")
        given_year = input(f"Enter desired year for ID: {city_id} Query: ")
        phase1.average_annual_temperature(self.connection, city_id,given_year)

    def get_average_seven_day_precipitation_menu(self):
        city_id = input(f"Enter desire City ID: ")
        start_date = input(f"Enter Start Date (YYYY-MM-DD) to query for City ID: {city_id} : ")
        phase1.average_seven_day_precipitation(self.connection,city_id,start_date)

    def get_average_mean_temp_by_city_menu(self):
        start_date = input("Enter Start Date (YYYY-MM-DD): ")
        end_date = input("Enter End Date (YYYY-MM-DD): ")
        phase1.average_mean_temp_by_city(self.connection, start_date,end_date)

    def get_average_annual_precipitation_by_country_menu(self):
        desired_year = input("Enter desired year (YYYY): ") 
        phase1.average_annual_precipitation_by_country(self.connection, desired_year) 
    # Phase 1 A. Basic Operations Ends here

    # Phase 2 -- B. Visualization Operations( Charts ) Starts here
    def plot_seven_day_prep_menu(self):
        phase2.bar_chart_7_day_prep(self.connection)
        
    def plot_prep_btw_dates_menu(self):
       phase2.bar_chart_prep_btw_dates(self.connection)

    def plot_yearly_prep_bar_chart_menu(self):
        phase2.bar_chart_annual_prep_by_countries(self.connection)

    def plot_temp_grouped_bar_chart_menu(self):
        phase2.grp_chart_temp_and_prep(self.connection)

    def plot_multiline_chart_menu(self):
        phase2.mtl_chart_min_max_temp_city(self.connection)

    def plot_scatterplot_chart_menu(self):
        phase2.sct_chart_annual_temp_by_prep(self.connection)
    # Phase 2 -- B. Visualization Operations( Charts )Ends here

    # Phase 4 -- C. Database Operations Starts here
    def db_operations_from_phase4(self):
        phase4.main(self.connection)
    # Phase 4 -- C. Database Operations Ends here
 
    

    def main(self):
        # While loop to keep application running until the quit option is selected 
        while True:
            self.display_menu()
            choice = input("Enter your choice (0-13): ")
            print("================================================================================")
            if choice == '1':
                self.get_all_countries_menu()
            elif choice == '2':
                self.get_all_cities_menu()
            elif choice == '3':
                self.get_average_annual_temperature_menu() 
            elif choice == '4':
                self.get_average_seven_day_precipitation_menu()
            elif choice == '5':
                self.get_average_mean_temp_by_city_menu()
            elif choice == '6':
                self.get_average_annual_precipitation_by_country_menu()
            elif choice == '7': 
                self.plot_seven_day_prep_menu()
            elif choice == '8':
                self.plot_prep_btw_dates_menu()
            elif choice == '9': 
                self.plot_yearly_prep_bar_chart_menu()
            elif choice == '10':
                self.plot_temp_grouped_bar_chart_menu()
            elif choice == '11':
                self.plot_multiline_chart_menu()
            elif choice == '12':
                self.plot_scatterplot_chart_menu()
            elif choice == '13': 
                self.db_operations_from_phase4()
            elif choice == '0':
                break
            else:
                print("Invalid choice. Please enter a valid option.")

if __name__ == "__main__":
    # ******************************************************************************#
    # Database path
    db_path='db\\CIS4044-N-SDI-OPENMETEO-PARTIAL.db'

    # Initiate database connection, using custom database connection module
    connection = con.connect_to_database(db_path)

    # Call main function with the database conection argument 
    interface = UserInterface(connection)
    interface.main()
    # Close database connection
    connection.close()
    # ******************************************************************************# 
    