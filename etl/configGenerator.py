from configparser import ConfigParser

config = ConfigParser()

config["database"]  = {
    "name" : "world.db",
    "createDBSqlPath" : "etl/database/create_city_database.sql",
    "insertDBDataSqlPath" : "etl/database/insert_city_data.sql",
    "getCityLocSqlPath" : "etl/database/fetch_city_loc.sql",
}

config["weatherbit"] = {
	"X-RapidAPI-Key": "d56eade46fmsh8ab961c3cb4d164p151caejsn49c71cd5c5df",
	"X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com",
    "url" : "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/daily",
    "tempExtractionPath" : "etl/data/temp/daily_forecast_raw_"
}

with open('weatherbit_etl_config.ini','w') as f:
    config.write(f)


