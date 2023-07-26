import sqlite3
import requests

#TODO: move all the static values to configuration file
headers = {
	"X-RapidAPI-Key": "d56eade46fmsh8ab961c3cb4d164p151caejsn49c71cd5c5df",
	"X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com"
}
url = "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/daily"
def extractWeatherData():
    conn = sqlite3.connect('world.db')
    c = conn.execute("""SELECT ID,Name,Latitude,Longitude FROM CITY where Latitude IS NOT NULL and Longitude is NOT NULL""")
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