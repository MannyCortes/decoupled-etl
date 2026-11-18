from fastapi import FastAPI, Response, UploadFile, File
from typing import Annotated
import logging
from dotenv import load_dotenv
import os
import redis
import time
import json
import boto3
from botocore.exceptions import ClientError

load_dotenv("app.env")
max_file_size = 50 * 1000 * 1000

aws_secret = os.getenv("aws_secret_access_key")
aws_access = os.getenv("aws_access_key_id")
bucket_name = os.getenv("bucket_name")
redis_url = os.getenv("redis_url")
s3 = boto3.session.Session(aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
r = redis.Redis.from_url(redis_url)
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', level=logging.INFO)
logging.getLogger("watchfiles").setLevel(logging.WARNING)


#Global Varaibles
max_attempts = 3
initial_delay = 1


#consider implementing hashing
app = FastAPI()
@app.post("/pipeline/upload")
def main(file: Annotated[UploadFile, File()]) -> Response:
    if file.size > max_file_size:
        return Response(content="File must be 50MB or less", status_code=400)
    file.file.seek(0)
    aws_key = f"uploads/staged/{file.filename}"
    try:
        upload_file(file, aws_key)
        redis_queue(file, aws_key)
        logger.info(f"File succesfully submitted")
        return Response(content=f"File received {file.filename}", status_code=200)
    except Exception as e:
        logger.exception(f"Exception raised: {e}")
        return Response(content="Unable to process request", status_code=500)


def upload_file(file: UploadFile, aws_key: str) -> bool: 
    retry_counter = 0
    while retry_counter < max_attempts:
        s3_client = s3.client('s3')
        try:
            #reset file pointer
            file.file.seek(0)
            s3_client.upload_fileobj(Fileobj=file.file, Bucket=bucket_name, Key=aws_key)
            return True
        except ClientError as e:
            retry_counter += 1
            delay = initial_delay * 2 ** retry_counter
            time.sleep(delay)
            logger.exception(f"Exception {e} raised on attempt. Retry: {retry_counter}")
            #grab e.response['Error']['Code'] == 'Whatever Error':
        except Exception as e:
            raise ConnectionError("An unexcpected error has occured")
    raise ConnectionError("Failed to upload file")
    
def redis_queue(file: UploadFile, aws_key: str) -> bool:
    redis_counter = 0
    while redis_counter < max_attempts:
        try:
            redis_payload = json.dumps({
            "filename": file.filename,
            "aws_key": aws_key,
            })
            r.rpush("submission_queue", redis_payload)
            return True
        except (redis.ConnectionError, redis.TimeoutError, redis.AuthenticationError) as e:
            redis_counter += 1
            logger.exception(f"Exception {e} raised on attempt. Retry: {redis_counter}")
            delay = initial_delay * 2 ** redis_counter
            time.sleep(delay)
        except (redis.exceptions.RedisError) as e:
            raise   
    raise ConnectionError("Failed to queue file in Redis")

    