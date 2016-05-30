import requests
import mysql.connector as mysql
import datetime
import time
import pandas as pd
import xml.etree.ElementTree as ET
from ConfigParser import ConfigParser

#get xml from url and turn into tree
url = 'https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml'
r = requests.get(url)
r = r.text
root = ET.fromstring(r)

#get time from xml root
timestamp = float(root.attrib['lastUpdate'])
timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp / 1000.0))

#parse data from tree
list = []
for child in root:
    list.append([item.text for item in child])

#convert to dataframe and give columns names
g = pd.DataFrame(list)
g.columns = ['id', 'name', 'terminalName', 'lat', 'long', 'installed', 'locked', 'installDate', 'removalDate', 'temporary', 'nbBikes', 'nbEmptyDocks', 'nbDocks']

#add timestamp column
g['timestamp'] = timestamp

#select relevant columns
output = g[['name', 'terminalName', 'installed', 'locked', 'temporary', 'nbBikes', 'nbEmptyDocks', 'nbDocks', 'timestamp']]

#get db creds from config.ini
config = ConfigParser()
config.read('/home/ubuntu/config.ini')

user = config.get('boris', 'user')
host = config.get('boris', 'host')
password = config.get('boris', 'password')
database = config.get('boris', 'database')
port = config.get('boris', 'port')

#connect to database
dbconn = mysql.connect(user = user, database = database, host = host, password = password)

#write dataframe to database
output.to_sql(con=dbconn, name = 'availability', if_exists = 'append', flavor = 'mysql', index = False)
