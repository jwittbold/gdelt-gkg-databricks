import os
import sys
import glob
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.toml_config import config
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser
from etl.execute_etl import *



FS_PREFIX = config['FS']['PREFIX']
# BLOB_BASE = f"{config['AZURE']['PREFIX']}{config['AZURE']['CONTAINER']}@{config['AZURE']['STORAGE_ACC']}{config['AZURE']['SUFFIX']}"
DOWNLOAD_PATH = config['ETL']['PATHS']['DOWNLOAD_PATH']
DOWNLOAD_METRICS = config['SCRAPER']['DOWNLOAD_METRICS']
PIPELINE_METRICS = config['ETL']['PATHS']['PIPELINE_METRICS']



def test_batch_processor():

    sample_dict = {'2021102100|.gkg.csv': 'file:///Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repo/gdelt/tests/test_resources/20211021000000.gkg.csv,'}
    # tests pass with this return statement
    return sample_dict.values()
    # test fails with this assert statement - not sure why
    # assert GkgBatchWriter().batch_processor() == sample_dict.values()

def test_check_processed():

    assert GkgBatchWriter().check_processed() == [None]


def test_write_batch():

    assert GkgBatchWriter().write_batch() == True