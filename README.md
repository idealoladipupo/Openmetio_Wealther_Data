# Steps to use Application
## Check / Install needed depencies by running:
- First, ensure python is properly installed on your machine
- Next run: 
```
pip install matplotlib 
```
```
pip install openmeteo-requests
```
```
pip install requests-cache retry-requests numpy pandas
```
 
## Testing the application
- Load root folder containing db and ICA in Vscode (recommended)
- From the root folder run:
 ```
py ICA/phase_3.py
```
- You are presented with a UI to perform common operations

## Common Operations Integerated in application
- Print all Countries ID and Name Data contained in database
- Print all City ID and Name Data contained in database
- Generate Average Annual Temperature for specific city for a given year
- Generate Average 7 day precipitation for a desired city from a specific date
- Generate Average mean temperature for all cities between two dates
- Generate Average Annual precipitation for all countries for a specific year
- Plot Bar Chart for Average seven day precipitation for cities from a specified date
- Plot Bar Chart for Average precipitation between specific dates
- Plot Bar Chart for Average Yearly precipitation for all countries
- Plot Group Bar Chart showing Min/Max temperatures and precipitation by Month for cities/country
- Scatter Plot for Average Temperature vs Average Precipitation
- Update database from Meteo Api for specific City between 2 Dates 

## Packages / Libraries Used  
- Matplotlib
- Datetime
- openmeteo-requests
- pandas
- numpy 
