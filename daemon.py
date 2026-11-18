import redis
import pandas as pd
import numpy as np
import logging
import boto3
from botocore.exceptions import ClientError
import os
import re
import json
from dotenv import load_dotenv
from typing import IO

load_dotenv("app.env")
redis_url = os.getenv("redis_url")
aws_access_key = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
aws_bucket = os.getenv("bucket_name")
r = redis.Redis.from_url(redis_url)
s3 = boto3.session.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_access_key)
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='daemon.log', level=logging.INFO)


regex_schema = {
    'trans_date_trans_time': r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', # YYYY-MM-DD HH:MM:SS
    'cc_num': r'^\d+$',                                               # Digits only
    'merchant': r'^.+$',                                              # Catch-all for varied punctuation/text
    'category': r'^[a-z_]+$',                                         # Lowercase and underscores
    'amt': r'^\d+(\.\d+)?$',                                          # Integer or float
    'first': r"^[A-Za-z\s\-']+$",                                     # Letters, spaces, hyphens, and apostrophes
    'last': r"^[A-Za-z\s\-']+$",                                      # Letters, spaces, hyphens, and apostrophes (e.g. O'Keefe)
    'gender': r'^[MF]$',                                              # Exactly M or F
    'street': r'^.+$',                                                # Catch-all for numbers, text, and punctuation
    'city': r'^[A-Za-z\s\-\.]+$',                                     # Letters, spaces, hyphens, and periods
    'state': r'^[A-Z]{2}$',                                           # 2-letter state abbreviation
    'zip': r'^\d{4,5}$',                                              # 4 to 5 digits (handles dropped leading zeros)
    'lat': r'^-?\d+\.\d+$',                                           # Float with optional negative sign
    'long': r'^-?\d+\.\d+$',                                          # Float with optional negative sign
    'city_pop': r'^\d+$',                                             # Digits only
    'job': r'^.+$',                                                   # Catch-all for varied punctuation (commas, slashes)
    'dob': r'^\d{4}-\d{2}-\d{2}$',                                    # YYYY-MM-DD
    'trans_num': r'^[a-f0-9]{32}$',                                   # 32-character hex string
    'unix_time': r'^\d+$',                                            # Digits only
    'merch_lat': r'^-?\d+\.\d+$',                                     # Float with optional negative sign
    'merch_long': r'^-?\d+\.\d+$',                                    # Float with optional negative sign
    'is_fraud': r'^[01]$'                                             # Exactly 0 or 1
}



#global variables                               
max_attempts = 3

     
def redis_daemon()-> dict:
    #holds client connection open until data arrives: "0" parameter
    counter = 0
    while True:
        try:
            if counter > max_attempts:
                return False 
            #python socket timeout, redis will return none after termination tcp socket and restart loop
            response = r.blmove("submission_queue", "processing_queue", 3, "LEFT", "RIGHT")
            if response != None:
                response = json.loads(response)
                return response
            else: continue      
        except (redis.ConnectionError, redis.TimeoutError, redis.AuthenticationError, redis.ResponseError) as e:
            counter += 1
            logging.exception(f"Error found at {e}")

def redis_decode(r_dict: dict) -> str:
    if r_dict.get("aws_key") != None:
        return r_dict.get("aws_key")
    else: 
        logging.warning("Unable to get aws_key")
        raise ValueError("Unable to get aws_key")


def fetch_file(aws_key) -> IO[bytes]:
    counter = 0
    response = s3.client('s3').get_object(Bucket=aws_bucket, Key=aws_key)
    while counter < max_attempts:
        try:
            return response["Body"]    
        except ClientError as e:
            counter += 1
            logging.exception(f"Failed to fetch object located at {aws_key} retry: {counter + 1} Error: {e}")
    raise ConnectionError(f"Failed to fetch object located at {aws_key}")


def create_df(stream_body: IO[bytes])->pd.DataFrame:
    #keep C default engine
    try:
        #default c engine is used, chunk generation is not supported due to multi treading processing
        df = pd.read_csv(stream_body, chunksize=1000, dtype_backend="pyarrow", usecols=list(regex_schema.keys()))
        return df
    except ValueError as e:
        logging.exception(f"Failed to create DataFrame: {e}")
        raise ValueError(e)


    
def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    boolean_mask = np.ones(len(chunk), dtype=bool)
    columns = chunk.columns.to_list()
    #df = chunk.dropna()
    print(columns)
    for col in columns:
        com_pattern = regex_schema[col]
        try: 
            d_type = chunk[col].dtype
            #filter by data types
            if "string[pyarrow]" in d_type:
                df_bool = chunk[col].str.match(com_pattern, na=False)
                boolean_mask = boolean_mask & df_bool
            #elif "" = #int                df_bool = df[col]#fill in
                #boolean_mask = boolean_mask & df_bool
            #apply final numpy array to dataframe
        except Exception as e:
            logging.exception(f"Unable to process dataframe chunk {e}")
    df = chunk[boolean_mask]     
    bad_df = chunk[~boolean_mask]
    return df       




def main():
    while True:
        try:
            r_dict = redis_daemon()
            if r_dict != False:
                aws_key = redis_decode(r_dict)
                stream_body = fetch_file(aws_key)
                df = create_df(stream_body)   
                logger.info("file succesfully received")
                for chunk in df:
                    regex_df = process_chunk(chunk) 
                    logger.info("Successfully processed a chunk")
                    print(regex_df)
        except Exception as e:
            #move the file into a seperate redis queue to move the object
            logging.exception(f"Unable to process file {e}")



if __name__ == "__main__":
    main()



        
