import redis
import logging
import os
import json
from dotenv import load_dotenv

load_dotenv("app.env")
redis_url = os.getenv("redis_url")
r = redis.Redis.from_url(redis_url)
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='daemon.log', level=logging.INFO)


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


def bucket_process(aws_key: str) -> bool:
    print("deez nuts")


def main():
    while True:
        try:
            r_dict = redis_daemon()
            if r_dict != False:
                aws_key = redis_decode(r_dict)
                logger.info("redis succesfully received")
                print(aws_key)
        except Exception as e:
            logging.error(f"Unable to commplete requests {e}")

if __name__ == "__main__":
    main()



        
