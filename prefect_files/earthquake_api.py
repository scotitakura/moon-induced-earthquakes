from urllib.request import urlopen
import json
import os
import snowflake.connector
from datetime import datetime
import pytz
import requests
import re
from bs4 import BeautifulSoup
from prefect import flow, task

@task
def open_connection():
    ctx = snowflake.connector.connect(
        user='scotitakura',
        password='Neokiki01!',
        account='cd28840.us-east4.gcp',
        warehouse='GEO_PROJECT',
        database='GEO_RAW',
        schema='GEO_SCHEMA'
        )
    cs = ctx.cursor()
    return cs

@task
def upload_hourly_earthquake(cs):
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
    response = urlopen(url)
    data_json = json.loads(response.read())

    for event in data_json['features'][::-1]:
        eq_time = event['properties']['time']
        eq_mag = event['properties']['mag']
        eq_place = event['properties']['place']
        eq_sources = event['properties']['sources']
        eq_type = event['properties']['type']
        eq_lat = event['geometry']['coordinates'][0]
        eq_lon = event['geometry']['coordinates'][1]
        eq_depth = event['geometry']['coordinates'][2]

        try:
            sql_statement = "INSERT INTO eq_table (eq_time, magnitude, place, sources, type, lat, lon, depth) VALUES ('" + str(eq_time) + "', " + str(eq_mag) + ", '" + eq_place + "', '" + eq_sources + "', '" + eq_type + "', '" + str(eq_lat) + "', '" + str(eq_lon) + "', '" + str(eq_depth) + "')"
            cs.execute(sql_statement)
        except:
            print('Except Error', sql_statement)

@task
def upload_hourly_moon_position(cs):
    URL = "https://www.timeanddate.com/astronomy/moon/light.html"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find_all("div", attrs={"class":"layout-grid__main"})
    td = results[0].find_all("td")
    lat_str = str(td[7]).replace('\\\'', '').replace('\xa0', ' ').replace('<td class="r">', '').replace('</td>', '')
    lon_str = str(td[10]).replace('\\\'', '').replace('\xa0', ' ').replace('<td class="r">', '').replace('</td>', '')
    DMSlat = lat_str + ' ' + str(td[8])[4]
    DMSlon = lon_str + ' ' + str(td[11])[4]
    deg, minutes, direction =  re.split('[°\']', DMSlat)
    lat = (float(deg) + float(minutes)/60) * (-1 if direction in ['W', 'S'] else 1)
    deg, minutes, direction =  re.split('[°\']', DMSlon)
    lon = (float(deg) + float(minutes)/60) * (-1 if direction in ['E', 'N'] else 1)

    try:
        sql_statement = "INSERT INTO moon_table (moon_time, lat, lon) VALUES ('" + str(datetime.now().astimezone(pytz.utc))[0:19] + "', '" + str(lat) + "', '" + str(lon) + "')"
        cs.execute(sql_statement)
    except:
        print('Except Error', sql_statement)

@task
def close_connection(cs):
    cs.close()
    
@flow(log_prints=True)
def run_pipeline():
    cs = open_connection()
    upload_hourly_earthquake(cs)
    upload_hourly_moon_position(cs)
    close_connection(cs)

if __name__ == "__main__":
    run_pipeline()