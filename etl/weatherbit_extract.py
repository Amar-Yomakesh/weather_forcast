import requests
import json
import pandas as pd
import os

url = "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/daily"

# querystring = {"lat":"38.5","lon":"-78.5"}
querystring = {"lat":"12.2979","lon":"76.6393"}


headers = {
	"X-RapidAPI-Key": "d56eade46fmsh8ab961c3cb4d164p151caejsn49c71cd5c5df",
	"X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
# write raw data to temporary file
with open("etl/data/temp/daily_forecast_raw.json","w") as rawData:
    rawData.write(response.text)
# get json object from raw data

with open('etl/data/temp/daily_forecast_raw.json') as file:
    data = file.read()
    weatherData=json.loads(data)

#initialise data set
city = weatherData["city_name"]
datetime = list()
high_temp = list()
low_temp = list()

# prepare data 
for weather in weatherData["data"]:
    datetime.append(weather["datetime"])
    high_temp.append(weather["high_temp"])
    low_temp.append(weather["low_temp"])
temperatureDf = pd.DataFrame({'datetime' : datetime,
                  'high_temp' : high_temp,
                  'low_temp' : low_temp})

# load extracted data into file
temperatureDf.to_csv(('etl/data/extract/daily_temperature_' + city +'.csv'),index=False)


os.remove('etl/data/temp/daily_forecast_raw.json')