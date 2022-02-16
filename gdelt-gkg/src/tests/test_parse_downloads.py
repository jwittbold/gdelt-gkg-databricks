import os
import sys
import datetime
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.parse_downloads import download_parser


gkg_file = '/Users/jackwittbold/Desktop/Springboard_Data_Engineering/Capstone_Master/gdelt_repo/gdelt/tests/test_resources/20211021000000.gkg.csv'

# print(download_parser(gkg_file))

def test_parse_downloads():

    assert download_parser(gkg_file) == ('20211021000000.gkg.csv', '20211021000000', datetime.datetime(2021, 10, 21, 0, 0), False, 
                                        Decimal('18.45052199999999942292561172507703304290771484375'), datetime.datetime(2022, 1, 26, 10, 19, 58))