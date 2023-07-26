import sqlite3
from configparser import ConfigParser

config = ConfigParser()

config.read("config_db.ini")

try:
    config_data = config["database"]
    print(config_data.get("name"))
except:
    print("config not found")
    exit(0)

# def createWorldDB():
#     conn = sqlite3.connect("world.db")
#     conn.cursor()
#     with open('etl/database/create_city_database.sql','r') as create_city_db:
#         sqlcityDBString = create_city_db.read()
#     conn.executescript(sqlcityDBString)
#     with open('etl/database/insert_city_data.sql','r',encoding='UTF8') as insert_city_data:
#         cityDataString = insert_city_data.read()
#     conn.executescript(cityDataString)
#     conn.commit()
#     conn.close()
#     return True
    
# createWorldDB()