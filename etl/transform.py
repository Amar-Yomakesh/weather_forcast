## connect to database
## loock up on city table. to fetch city Id
#run pip install mysql-connector-python
from os import remove
from os import listdir
from os.path import isfile, join
import re
import pandas as pd
import json

tempDirectory = 'etl/data/temp/'
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
      temperatureDf.to_csv(('etl/data/extract/daily_temperature_' + city_name + '_' + city_id+'.csv'),index=False)
    remove(tempDirectory + file)

 


# city_daily_temperature_files = [f for f in listdir('etl/data/extract/') if re.match('daily_temperature_',f)]

# for file in city_daily_temperature_files:
#     city_name = ((file.split('_')[2]).split('.')[0])
#     world_db_cursor = world_db.cursor()
#     world_db_cursor.execute("SELECT ID FROM world.city where name = '" + city_name +"'")
#     city_id = (world_db_cursor.fetchone())[0]
#     temperature_df = pd.read_csv(('etl/data/extract/'+file))
#     temperature_df["city_id"] = city_id
#     temperature_df.to_csv("etl/data/extract/"+file,index=False)
    
    

