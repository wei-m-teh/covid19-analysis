import requests
import json
import pandas as pd
import csv
import boto3
from io import StringIO

def lambda_handler(event, context):
    r = requests.get("https://coronavirus-tracker-api.herokuapp.com/v2/locations?country_code=US&source=csbs")
    output = r.json()
    data = {}
    country = []
    state = []
    county = []
    lat = []
    long = []
    confirmed = []
    deaths = []
    recovered = []
    for l in output['locations']:
        country.append(l['country'])
        state.append(l['province'])
        county.append(l['county'])
        lat.append(l['coordinates']['latitude'])
        long.append(l['coordinates']['longitude'])
        confirmed.append(l['latest']['confirmed'])
        deaths.append(l['latest']['deaths'])
        recovered.append(l['latest']['recovered'])
        
    data['country'] = country
    data['state'] = state
    data['county'] = county
    data['lat'] = lat
    data['long'] = long
    data['confirmed'] = confirmed
    data['deaths'] = deaths
    data['recovered'] = recovered
    df = pd.DataFrame(data = data)
    
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
    
    s3_resource = boto3.resource("s3")
    s3_resource.Object("weteh-data-repo", "covid-19/time_series/csv/us_combined/us_combined.csv").put(Body=csv_buffer.getvalue())
    return {
        'statusCode': 200,
        'body': json.dumps('Completed COVID-19 Analysis for US')
    }

