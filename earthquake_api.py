from urllib.request import urlopen
import json
import os
import snowflake.connector
from prefect import flow, task
from prefect_dbt.cloud import DbtCloudCredentials, DbtCloudJob
from prefect_dbt.cloud.jobs import run_dbt_cloud_job

DbtCloudCredentials(
    api_key="f5586836d14b4b65586d8e147f44f0cc26214870",
    account_id="127301"
).save("scot_dbt_credentials")

dbt_cloud_credentials = DbtCloudCredentials.load("scot_dbt_credentials")
dbt_cloud_job = DbtCloudJob.load(
    dbt_cloud_credentials=dbt_cloud_credentials,
    job_id="172142"
)

DbtCloudJob.load("scot_dbt_credentials")

url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
response = urlopen(url)
data_json = json.loads(response.read())

@task
def upload_hourly_earthquake():
    ctx = snowflake.connector.connect(
        user='scotitakura',
        password='Neokiki01!',
        account='cd28840.us-east4.gcp',
        warehouse='GEO_PROJECT',
        database='GEO_RAW',
        schema='GEO_SCHEMA'
        )
    cs = ctx.cursor()

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

    cs.close()
    ctx.close()

@flow(log_prints=True, name="prefect-example-hellow-flow")
def run_pipeline():
    upload_hourly_earthquake()

if __name__ == "__main__":
    run_pipeline()