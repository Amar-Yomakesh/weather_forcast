CREATE TABLE IF NOT EXISTS CITY (
  ID int PRIMARY KEY,
  Name char(35) NOT NULL DEFAULT '',
  CountryCode char(3) NOT NULL DEFAULT '',
  District char(20) NOT NULL DEFAULT '',
  Population int NOT NULL DEFAULT '0',
  Latitude varchar(45) DEFAULT NULL,
  Longitude varchar(45) DEFAULT NULL
)WITHOUT ROWID;