from urllib.request import urlopen
import json
import os
import snowflake


url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
response = urlopen(url)
data_json = json.loads(response.read())

eq_properties = data_json['features'][0]['properties']

ctx = snowflake.connector.connect(
    user='scotitakura',
    password='Neokiki01!',
    account='TP35935'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()