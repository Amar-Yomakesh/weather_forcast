from configparser import ConfigParser
import sqlite3
import requests
from datetime import datetime

config = ConfigParser()
try:
    config.read("weatherbit_etl_config.ini")
    config_db = config["database"]
    config_weatherbit = config["weatherbit"]
except:
    print("config not found")
    exit(0)

# initialise rapid api connection parameters
headers = {
	"X-RapidAPI-Key": config_weatherbit.get("x-rapidapi-key"),
	"X-RapidAPI-Host": config_weatherbit.get("x-rapidapi-host")
}
url = config_weatherbit.get("url")
output_name = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
def main():
    print(output_name)
    # createWorldDB()
    # extractWeatherData()



def createWorldDB():
    conn = sqlite3.connect(config_db.get("name"))
    conn.cursor()
    with open(config_db.get("createdbsqlpath"),'r') as create_city_db:
        sqlcityDBString = create_city_db.read()
    conn.executescript(sqlcityDBString)
    with open(config_db.get("insertdbdatasqlpath"),'r',encoding='UTF8') as insert_city_data:
        cityDataString = insert_city_data.read()
    conn.executescript(cityDataString)
    conn.commit()
    conn.close()
    return True



def extractWeatherData():
    conn = sqlite3.connect(config_db.get("name"))
    c = conn.executescript(config_db.get("getcitylocsqlpath"))
    extractCount = 0
    for record in c.fetchall():
        (city_id,city_name,city_lat,city_long) = (record[0],record[1],record[2],record[3])
        querystring = {"lat":city_lat,"lon":city_long}
        response = requests.get(url, headers=headers, params=querystring)
        with open("etl/data/temp/daily_forecast_raw_" + city_name + "_" + str(city_id)+ ".json","w") as rawFile:
            rawFile.write(response.text)
        extractCount +1
    conn.close()
    if extractCount >0:
        return True
    else:
        False

main()