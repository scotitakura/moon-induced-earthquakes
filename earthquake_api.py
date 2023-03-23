from urllib.request import urlopen
import json
import os
import snowflake.connector
import datetime
import requests
from bs4 import BeautifulSoup
from astropy import units as u
from astropy.coordinates import SkyCoord
from prefect import flow, task
from prefect_dbt.cli import DbtCoreOperation
from prefect_dbt.cloud import DbtCloudCredentials, DbtCloudJob
from prefect_dbt.cloud.jobs import run_dbt_cloud_job

#DbtCloudCredentials(api_key="f5586836d14b4b65586d8e147f44f0cc26214870",
#                    account_id="127301"
#                    ).save("scot-dbt-credentials", overwrite=True)

#dbt_cloud_credentials = DbtCloudCredentials.load("scot-dbt-credentials")
#dbt_cloud_job = DbtCloudJob.load(dbt_cloud_credential, "245496")

url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
response = urlopen(url)
data_json = json.loads(response.read())

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

@task
def upload_hourly_earthquake():

    for event in data_json['features'][::-1]:
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
            print('Except Error', sql_statement)

@task
def upload_hourly_moon_position():
    URL = "https://theskylive.com/quickaccess?objects=moon&data=equatorial-time"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find_all("div", class_="keyinfobox")
    i=0
    for job_element in results:
        i+=1
        if i == 4:
            declination = job_element.find("ar").text
        if i == 5:
            right_ascension = job_element.find("ar").text
            
    right_ascension_format = right_ascension.replace('°', '').replace('’', '').replace('”', '')
    c = SkyCoord(declination + ' ' + right_ascension_format, unit=(u.hourangle, u.deg))
    c.replace('<SkyCoord (ICS): (ra, dec) in deg(', '').replace(')', '')

    try:
        sql_statement = "INSERT INTO eq_table (datetime, declination, right_ascencion, lat_lon) VALUES (" + "'" + str(eq_time) + "', " + str(eq_mag) + ", '" + eq_place + "', '" + eq_sources + "', '" + eq_type + "', '" + str(eq_coordinates) + "')"
        cs.execute(sql_statement)
        one_row = cs.fetchone()
    except:
        print('Except Error', sql_statement)

@task
def close_connection():
    cs.close()
    ctx.close()
    

@flow(log_prints=True, name="prefect-example-hellow-flow")
def run_pipeline() -> str:
    open_connection()
    upload_hourly_earthquake()
    upload_hourly_moon_position()
    close_connection()

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