import pandas as pd
import requests
import boto3
from io import StringIO
import csv
import json

confirmed_raw_csv_file = "time_series_19-covid-Confirmed.csv"
deaths_raw_csv_file = "time_series_19-covid-Deaths.csv"
recovered_raw_csv_file = "time_series_19-covid-Recovered.csv"

def lambda_handler(event, context):
   r_confirmed = requests.get("https://raw.github.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/{}".format(confirmed_raw_csv_file))
   r_deaths = requests.get("https://raw.github.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/{}".format(deaths_raw_csv_file))
   r_recovered = requests.get("https://raw.github.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/{}".format(recovered_raw_csv_file))

   with open("/tmp/{}".format(confirmed_raw_csv_file), 'w') as o:
      o.write(r_confirmed.text)

   with open("/tmp/{}".format(deaths_raw_csv_file), 'w') as o:
      o.write(r_deaths.text)

   with open("/tmp/{}".format(recovered_raw_csv_file), 'w') as o:
      o.write(r_recovered.text)

   confirmed_df = pd.read_csv("/tmp/{}".format(confirmed_raw_csv_file))
   confirmed_df_melt = confirmed_df.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name="Date", value_name="Confirmed")

   deaths_df = pd.read_csv("/tmp/{}".format(deaths_raw_csv_file))
   deaths_df_melt = deaths_df.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name="Date", value_name="Deaths")

   recovered_df = pd.read_csv("/tmp/{}".format(recovered_raw_csv_file))
   recovered_df_melt = recovered_df.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name="Date", value_name="Recovered")
   join1_df = confirmed_df_melt.join(deaths_df_melt.set_index(["Province/State", "Country/Region", "Lat", "Long", "Date"]), on=["Province/State", "Country/Region", "Lat", "Long", "Date"])

   join2_df = join1_df.join(recovered_df_melt.set_index(["Province/State", "Country/Region", "Lat", "Long", "Date"]), on=["Province/State", "Country/Region", "Lat", "Long", "Date"])
   join2_df['Date'] = join2_df.Date.apply(lambda x: x + "20")

   s3 = boto3.client('s3')
   csv_buffer = csv_buffer = StringIO()
   join2_df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)

   s3_resource = boto3.resource("s3")
   s3_resource.Object("weteh-data-repo", "covid-19/time_series/csv/combined/combined.csv").put(Body=csv_buffer.getvalue())

   with open("/tmp/{}".format(confirmed_raw_csv_file), "rb") as f:
      s3.upload_fileobj(f, "weteh-data-repo", "covid-19/time_series/csv/confirmed/{}".format(confirmed_raw_csv_file))

   with open("/tmp/{}".format(deaths_raw_csv_file), "rb") as f:
      s3.upload_fileobj(f, "weteh-data-repo", "covid-19/time_series/csv/deaths/{}".format(deaths_raw_csv_file))

   with open("/tmp/{}".format(recovered_raw_csv_file), "rb") as f:
      s3.upload_fileobj(f, "weteh-data-repo", "covid-19/time_series/csv/recovered/{}".format(recovered_raw_csv_file))
      
   return {
       'statusCode': 200,
       'body': json.dumps('Completed COVID-19 data analysis from Lambda!')
   }


