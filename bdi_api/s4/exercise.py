from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

import boto3

import time
import requests
import json
import glob
import os


settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
                Limits the number of files to download.
                You must always start from the first the page returns and
                go in ascending order in order to correctly obtain the results.
                I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    # TODO


    downloaded = 0
    counter = 0
    suffix_url = "Z.json.gz"

    s3_client = boto3.client("s3")


    while downloaded < file_limit:
        
        time.sleep(2)
        
        try:

            response = requests.get(f"{base_url}{counter:06d}{suffix_url}")

            if response.status_code == 200:
                
                data = response.json() 
                print(f"File {counter:06d} parsed successfully.")
                
                # Uploading to S3
                s3_key = f"{s3_prefix_path}{counter:06d}.json"
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=json.dumps(data),
                    ContentType="application/json"
                )

                print(f"files uploaded to s3://{s3_bucket}/{s3_key}")
                downloaded += 1

            else:

                print(f"File {counter:06d} not found (Status {response.status_code})")
        
        except Exception as e:
        
            print(f"Error processing {counter:06d}: {e}")
            break;
        
        counter += 5



    return "OK"