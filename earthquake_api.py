from urllib.request import urlopen
import json
import os
import snowflake.connector


url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
response = urlopen(url)
data_json = json.loads(response.read())

ctx = snowflake.connector.connect(
    user='scotitakura',
    password='Neokiki01!',
    account='cd28840.us-east4.gcp',
    warehouse='GEO_PROJECT',
    database='GEO_RAW',
    schema='GEO_SCHEMA'
    )
cs = ctx.cursor()

for event in data_json['features']:
    eq_time = event['properties']['time']
    eq_mag = event['properties']['mag']
    eq_place = event['properties']['place']
    eq_sources = event['properties']['sources']
    eq_type = event['properties']['type']
    eq_coordinates = event['geometry']['coordinates']

    try:
        sql_statement = "INSERT INTO eq_table (eq_time, magnitude, place, sources, type, coordinates) VALUES (" + "'" + str(eq_time) + "', " + str(eq_mag) + ", '" + eq_place + "', '" + eq_sources + "', '" + eq_type + "', '" + str(eq_coordinates) + "')"
        cs.execute(sql_statement)
        one_row = cs.fetchone()
    except:
        print('didnt work', sql_statement)


cs.close()
ctx.close()