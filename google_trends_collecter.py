import json
from datetime import datetime 
from time import sleep
import pandas as pd
import argparse
import boto3
import os

from pytrends.request import TrendReq

def set_job():
    parser = argparse.ArgumentParser(description='Collect tweets')
    parser.add_argument('--configuration', type=str,  help='Configuration file for the job', default="./configuration.json")
    parser.add_argument('--candidates', type=str,  help='Configuration file for the job', default="./candidates.json")
    parser.add_argument('--area', type=str,  help='area for study', default="fr")
    parser.add_argument('--period', type=str,  help='time period', default="hourly")
    

    args = parser.parse_args()

    with open(args.configuration) as f:
        configuration = json.load(f)

    with open(args.candidates) as f:
        candidates = json.load(f)

    return configuration, candidates, args.area, args.period

file_extension = ".csv.gz"
if __name__ == '__main__':
    configuration, candidates, area, period = set_job()
    date_collect = datetime.utcnow().strftime("%Y%m%d_%H%M")
    partition = datetime.utcnow().strftime('%Y%m%d')

    timeframe = "now 1-H"
    prefix = ""
    wait_time = 5
    if period == "daily":
        timeframe = "now 1-d"
        prefix = "daily_"
        wait_time = 60
    elif period == "weekly":
        timeframe = "now 7-d"
        prefix = "weekly_"
        wait_time = 60

    s3_client = boto3.client('s3', aws_access_key_id=configuration["aws"]["key"], aws_secret_access_key=configuration["aws"]["secret"])
    if area == "fr":
        pytrends = TrendReq(hl='fr-FR', tz=360, timeout=(5,10))
    else:
        pytrends = TrendReq(tz=360, timeout=(5,10))

    for key, item in candidates.items():
        print(key, item["name"])
        kw_list = [item["name"]]
        file_name = f"{key}_{date_collect}{file_extension}"

        if area == "fr":
            pytrends.build_payload(kw_list, cat=0, timeframe=timeframe, geo='FR')
        else:
            pytrends.build_payload(kw_list, cat=0, timeframe=timeframe)
        
        # Get the interest over time
        dfp_iot = pytrends.interest_over_time()
        if len(dfp_iot) > 0:
            dfp_iot.columns = ["interest", "is_partial"]
            dfp_iot.reset_index(inplace=True)
            dfp_iot["candidate"] = key
            dfp_iot["date_collect"] = date_collect
            dfp_iot.to_csv("tmp_iot.csv.gz", index=None)
            
            # Upload the file to s3
            response = s3_client.upload_file("tmp_iot.csv.gz", configuration["aws"]["bucket"], f'data/raw/google_trends/{area}/{prefix}interest_over_time/{partition}/{file_name}')
            
        # Get the interest on region
        dfp_ibr = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=False)
        if len(dfp_ibr) > 0:
            dfp_ibr.columns = ["interest"]
            dfp_ibr.reset_index(inplace=True)
            dfp_ibr["candidate"] = key
            dfp_ibr["date_collect"] = date_collect
            dfp_ibr.to_csv("tmp_ibr.csv.gz", index=None)

            # Upload the file to s3
            response = s3_client.upload_file("tmp_ibr.csv.gz", configuration["aws"]["bucket"], f'data/raw/google_trends/{area}/{prefix}interest_by_region/{partition}/{file_name}')

        dict_related_topics = pytrends.related_topics()
        for key_rt, dfp_rt in dict_related_topics[kw_list[0]].items():
            if isinstance(dfp_rt, pd.DataFrame):
                dfp_rt["candidate"] = key
                dfp_rt["date_collect"] = date_collect
                dfp_rt.to_csv("tmp_rt.csv.gz", index=None)

                # Upload the file to s3
                response = s3_client.upload_file("tmp_rt.csv.gz", configuration["aws"]["bucket"], f'data/raw/google_trends/{area}/{prefix}related_topics_{key_rt}/{partition}/{file_name}')

        dict_related_queries = pytrends.related_queries()
        for key_rq, dfp_rq in dict_related_queries[kw_list[0]].items():
            if isinstance(dfp_rq, pd.DataFrame):
                dfp_rq["candidate"] = key
                dfp_rq["date_collect"] = date_collect
                dfp_rq.to_csv("tmp_rq.csv.gz", index=None)

                # Upload the file to s3
                response = s3_client.upload_file("tmp_rq.csv.gz", configuration["aws"]["bucket"], f'data/raw/google_trends/{area}/{prefix}related_queries_{key_rq}/{partition}/{file_name}')

        sleep(wait_time)
        # break