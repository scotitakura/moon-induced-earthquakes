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
from prefect.deployments import Deployment
from prefect_dbt.cli import DbtCoreOperation
from prefect_dbt.cloud import DbtCloudCredentials, DbtCloudJob
from prefect_dbt.cloud.jobs import run_dbt_cloud_job

#DbtCloudCredentials(api_key="f5586836d14b4b65586d8e147f44f0cc26214870",
#                    account_id="127301"
#                    ).save("scot-dbt-credentials", overwrite=True)

#dbt_cloud_credentials = DbtCloudCredentials.load("scot-dbt-credentials")
#dbt_cloud_job = DbtCloudJob.load(dbt_cloud_credential, "245496")

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
        eq_coordinates = event['geometry']['coordinates']

        try:
            sql_statement = "INSERT INTO eq_table (eq_time, magnitude, place, sources, type, coordinates) VALUES ('" + str(eq_time) + "', " + str(eq_mag) + ", '" + eq_place + "', '" + eq_sources + "', '" + eq_type + "', '" + str(eq_coordinates) + "')"
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
    DMSlat = str(td[7])[14:-6] + ' ' + str(td[8])[4]
    DMSlon = str(td[10])[14:-6] + ' ' + str(td[11])[4]
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

#    run_dbt_cloud_job(
#        dbt_cloud_job=DbtCloudJob.load(dbt_cloud_credentials, "245496")
#    ).run()

#    DbtCoreOperation(
#        commands=["pwd", "dbt debug", "dbt run"],
#        project_dir="/home/scotitakura/.dbt/moon_project/moon_eq/",
#        profiles_dir="/home/scotitakura/.dbt/"
#    ).run()

if __name__ == "__main__":
    run_pipeline()