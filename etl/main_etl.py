from configparser import ConfigParser
import sqlite3
import requests
from datetime import datetime
from os import remove
from os import listdir
import re
import pandas as pd
import json

#get configuration parameters
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
output_name = datetime.now().strftime("%Y%m%d%H%M%S")

def main():
    createWorldDB()
    extractWeatherData()
    transform_write_CSV()

#create world.db and load city information
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

#call weatherbit api for each of the city and write the raw json into files
def extractWeatherData():
    conn = sqlite3.connect(config_db.get("name"))
    c = conn.execute("""SELECT ID,Name,Latitude,Longitude FROM CITY where Latitude IS NOT NULL and Longitude is NOT NULL""")
    records = c.fetchall()
    tempExtractionFilePath = config.get("weatherbit","tempextractionpath") + "daily_forecast_raw_"
    extractCount = 0
    for record in records:
        (city_id,city_name,city_lat,city_long) = (record[0],record[1],record[2],record[3])
        querystring = {"lat":city_lat,"lon":city_long}
        response = requests.get(url, headers=headers, params=querystring)
        with open(tempExtractionFilePath + city_name + "_" + str(city_id)+ ".json","w") as rawFile:
            rawFile.write(response.text)
        extractCount +1
    conn.close()
    if extractCount >0:
        return True
    else:
        False

#TODO performance tuning. too many for loops?
#read raw file and prepare csv 
def transform_write_CSV():
    tempDirectory = config.get("weatherbit","tempextractionpath")
    extractCSVFile = config.get("weatherbit","extractCSVPath") + "daily_temperature_"
    rawFiles = [f for f in listdir(tempDirectory) if re.match('daily_forecast_raw_*.*.json',f)]
    for file in rawFiles:
        with open((tempDirectory + file), 'r') as rawFile:
            weatherData = json.loads(rawFile.read())
        city_name = ((file.split('_')[3]))
        city_id = ((file.split('_')[4]).split('.')[0])
        datetime = list()
        high_temp = list()
        low_temp = list()
        for weather in weatherData["data"]:
            datetime.append(weather["datetime"])
            high_temp.append(weather["high_temp"])
            low_temp.append(weather["low_temp"])
        temperatureDf = pd.DataFrame({'datetime' : datetime,
                            'high_temp' : high_temp,
                            'low_temp' : low_temp,
                            'city_id':city_id})
        temperatureDf.to_csv((extractCSVFile + city_name + '_' + city_id + '_'+ output_name+'.csv'),index=False)
        remove(tempDirectory + file)

if __name__ == "__main__":
    main()