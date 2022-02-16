import os
import sys
import csv
import glob
import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, \
     DateType, TimestampType, StructType, StructField, ArrayType, BooleanType, FloatType, DoubleType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from config.toml_config import config
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import *


# FS_PREFIX = f"{config['FS']['PREFIX']}"

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('write_transformed_gkg') \
    .getOrCreate()

data_file = 'file:///Users/jackwittbold/Desktop/gdelt_data/TEST_RUN/raw_gkg/20211021000000.gkg.csv'
raw_gkg = spark.sparkContext.textFile(data_file)
parsed = raw_gkg.map(lambda Row: gkg_parser(Row))
df = spark.createDataFrame(parsed, schema=gkg_schema)




csv_file = '/Users/jackwittbold/Desktop/gdelt_data/TEST_RUN/raw_gkg/20211021000000.gkg.csv'

with open(csv_file, newline='', encoding='ISO-8859-1') as f:

    row_list = []
    csv_data = csv.reader(f, delimiter='\t')
    for row in csv_data:
        row_list.append(row)
first_row = row_list[0]


df.select('GkgRecordId').show(truncate=False)
def test_create_gkg_record_id():

    assert create_gkg_record_id(first_row[0]) == (20211021000000, False, 0)


# df.select('V21Date').show(truncate=False)
def test_create_v21_date():

    assert create_v21_date(first_row[1]) == Row(datetime.datetime.strptime('20211021000000', '%Y%m%d%H%M%S'))


# df.select('V2SrcCollectionId').show(truncate=False)
def test_create_v2_src_collection_id():

    assert create_v2_src_collection_id(first_row[2]) == Row('WEB')


# df.select('V2SrcCmnName').show(truncate=False)
def test_create_v2_src_common_name():

    assert create_v2_src_common_name(first_row[3]) == Row('bryantimes.com')


# df.select('V2DocId').show(truncate=False)
def test_create_v2_doc_id():

    assert create_v2_doc_id(first_row[4]) == Row('https://www.bryantimes.com/news/local/brown-supporting-child-suicide-prevention-bill/article_cd0acb20-a05e-5cf3-b9dc-3949a8746dfb.html')


# V1Count Array
# df.select('V1Counts').show(truncate=False)
def test_create_v1_count_array():

    assert create_v1_count_array(first_row[5]) == [('KILL', 13, '', 1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US'), \
                                                ('CRISISLEX_T03_DEAD', 13, '', 1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US'), \
                                                ('CRISISLEX_T03_DEAD', 13, '', 1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US')]


# V21Count Array
# df.select('V21Counts').show(truncate=False)
def test_create_v21_count_array():

    assert create_v21_count_array(first_row[6]) == [('KILL', 13, '', 1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 1402), \
                                                ('CRISISLEX_T03_DEAD', 13, '', 1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 1402), \
                                                ('CRISISLEX_T03_DEAD', 13, '', 1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 1402)]



# df.select('V1Themes').show(truncate=False)
def test_create_v1_themes_array():

    assert create_v1_themes_array(first_row[7]) == Row(['MEDIA_MSM', 'TAX_FNCACT', 'TAX_FNCACT_CHILD', 'SOC_SUICIDE', 'WB_2024_ANTI_CORRUPTION_AUTHORITIES', \
                                                        'WB_696_PUBLIC_SECTOR_MANAGEMENT', 'WB_831_GOVERNANCE', 'WB_832_ANTI_CORRUPTION', 'WB_2026_PREVENTION', \
                                                        'CRISISLEX_CRISISLEXREC', 'TAX_ETHNICITY', 'TAX_ETHNICITY_AMERICANS', 'GENERAL_HEALTH', 'HEALTH_PANDEMIC', \
                                                        'WB_635_PUBLIC_HEALTH', 'WB_2165_HEALTH_EMERGENCIES', 'WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE', \
                                                        'WB_621_HEALTH_NUTRITION_AND_POPULATION', 'WB_2167_PANDEMICS', 'UNGP_HEALTHCARE', 'DEMOCRACY', 'LEADER', \
                                                        'TAX_FNCACT_CONGRESSWOMAN', 'USPEC_POLITICS_GENERAL1', 'MEDICAL', 'TAX_FNCACT_DOCTORS', 'CRISISLEX_C03_WELLBEING_HEALTH', \
                                                        'WB_1406_DISEASES', 'WB_1430_MENTAL_HEALTH', 'WB_1427_NON_COMMUNICABLE_DISEASE_AND_INJURY', 'UNGP_FORESTS_RIVERS_OCEANS', \
                                                        'UNGP_CRIME_VIOLENCE', 'EPU_CATS_HEALTHCARE', 'TAX_ETHNICITY_AMERICAN', 'SOC_POINTSOFINTEREST', 'SOC_POINTSOFINTEREST_HOSPITAL', \
                                                        'EDUCATION', 'SOC_POINTSOFINTEREST_COLLEGES', 'TAX_FNCACT_CHILDREN', 'TAX_ETHNICITY_BLACK', 'KILL', 'CRISISLEX_T03_DEAD', \
                                                        'TAX_FNCACT_PEERS', 'TAX_FNCACT_PRINCIPAL', 'TAX_FNCACT_INVESTIGATOR', 'AFFECT', 'WB_2670_JOBS', 'WB_1467_EDUCATION_FOR_ALL', \
                                                        'WB_470_EDUCATION', 'WB_2131_EMPLOYABILITY_SKILLS_AND_JOBS', 'WB_1484_EDUCATION_SKILLS_DEVELOPMENT_AND_LABOR_MARKET', \
                                                        'SOC_POINTSOFINTEREST_SCHOOLS'])


# df.select('V2EnhancedThemes').show(truncate=False)
def test_create_v2_enhanced_themes_array():

    assert create_v2_enhanced_themes_array(first_row[8]) == [
        ('MEDIA_MSM', 40), ('EDUCATION', 1036), ('SOC_POINTSOFINTEREST_COLLEGES', 1036), ('TAX_FNCACT_CHILD', 109), ('TAX_FNCACT_CHILD', 511), ('TAX_FNCACT_CHILD', 859), \
        ('TAX_FNCACT_CHILD', 2599), ('KILL', 1457), ('CRISISLEX_T03_DEAD', 1457), ('SOC_POINTSOFINTEREST_HOSPITAL', 977), ('SOC_POINTSOFINTEREST_HOSPITAL', 1733), \
        ('CRISISLEX_CRISISLEXREC', 210), ('CRISISLEX_CRISISLEXREC', 1899), ('TAX_FNCACT_INVESTIGATOR', 1612), ('TAX_ETHNICITY_AMERICANS', 224), \
        ('SOC_POINTSOFINTEREST_SCHOOLS', 2297), ('SOC_SUICIDE', 117), ('SOC_SUICIDE', 165), ('SOC_SUICIDE', 402), ('SOC_SUICIDE', 519), ('SOC_SUICIDE', 718), \
        ('SOC_SUICIDE', 1101), ('SOC_SUICIDE', 1380), ('SOC_SUICIDE', 1468), ('SOC_SUICIDE', 1638), ('SOC_SUICIDE', 1792), ('SOC_SUICIDE', 1859), ('SOC_SUICIDE', 2769), \
        ('GENERAL_HEALTH', 308), ('GENERAL_HEALTH', 2038), ('GENERAL_HEALTH', 2169), ('HEALTH_PANDEMIC', 308), ('HEALTH_PANDEMIC', 2038), ('HEALTH_PANDEMIC', 2169), \
        ('WB_635_PUBLIC_HEALTH', 308), ('WB_635_PUBLIC_HEALTH', 2038), ('WB_635_PUBLIC_HEALTH', 2169), ('WB_2165_HEALTH_EMERGENCIES', 308), ('WB_2165_HEALTH_EMERGENCIES', 2038), \
        ('WB_2165_HEALTH_EMERGENCIES', 2169), ('WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE', 308), \
        ('WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE', 2038), ('WB_2166_HEALTH_EMERGENCY_PREPAREDNESS_AND_DISASTER_RESPONSE', 2169), \
        ('WB_621_HEALTH_NUTRITION_AND_POPULATION', 308), ('WB_621_HEALTH_NUTRITION_AND_POPULATION', 2038), ('WB_621_HEALTH_NUTRITION_AND_POPULATION', 2169), \
        ('WB_2167_PANDEMICS', 308), ('WB_2167_PANDEMICS', 2038), ('WB_2167_PANDEMICS', 2169), ('UNGP_HEALTHCARE', 308), ('UNGP_HEALTHCARE', 2038), ('UNGP_HEALTHCARE', 2169), \
        ('WB_2024_ANTI_CORRUPTION_AUTHORITIES', 128), ('WB_2024_ANTI_CORRUPTION_AUTHORITIES', 530), ('WB_2024_ANTI_CORRUPTION_AUTHORITIES', 1112), \
        ('WB_2024_ANTI_CORRUPTION_AUTHORITIES', 1649), ('WB_696_PUBLIC_SECTOR_MANAGEMENT', 128), ('WB_696_PUBLIC_SECTOR_MANAGEMENT', 530), \
        ('WB_696_PUBLIC_SECTOR_MANAGEMENT', 1112), ('WB_696_PUBLIC_SECTOR_MANAGEMENT', 1649), ('WB_831_GOVERNANCE', 128), ('WB_831_GOVERNANCE', 530), ('WB_831_GOVERNANCE', 1112), \
        ('WB_831_GOVERNANCE', 1649), ('WB_832_ANTI_CORRUPTION', 128), ('WB_832_ANTI_CORRUPTION', 530), ('WB_832_ANTI_CORRUPTION', 1112), ('WB_832_ANTI_CORRUPTION', 1649), \
        ('WB_2026_PREVENTION', 128), ('WB_2026_PREVENTION', 530), ('WB_2026_PREVENTION', 1112), ('WB_2026_PREVENTION', 1649), ('WB_2670_JOBS', 2270), \
        ('WB_1467_EDUCATION_FOR_ALL', 2270), ('WB_470_EDUCATION', 2270), ('WB_2131_EMPLOYABILITY_SKILLS_AND_JOBS', 2270), ('WB_1484_EDUCATION_SKILLS_DEVELOPMENT_AND_LABOR_MARKET', 2270), \
        ('TAX_ETHNICITY_AMERICAN', 931), ('TAX_ETHNICITY_AMERICAN', 968), ('TAX_ETHNICITY_AMERICAN', 1019), ('AFFECT', 2182), ('TAX_FNCACT_PEERS', 1491), ('UNGP_CRIME_VIOLENCE', 706), \
        ('LEADER', 469), ('TAX_FNCACT_CONGRESSWOMAN', 469), ('USPEC_POLITICS_GENERAL1', 469), ('MEDICAL', 589), ('TAX_FNCACT_DOCTORS', 589), ('CRISISLEX_C03_WELLBEING_HEALTH', 589), \
        ('EPU_CATS_HEALTHCARE', 788), ('EPU_CATS_HEALTHCARE', 1150), ('TAX_FNCACT_PRINCIPAL', 1599), ('DEMOCRACY', 455), ('WB_1406_DISEASES', 614), ('WB_1430_MENTAL_HEALTH', 614), \
        ('WB_1427_NON_COMMUNICABLE_DISEASE_AND_INJURY', 614), ('TAX_FNCACT_CHILDREN', 1277), ('TAX_FNCACT_CHILDREN', 1724), ('UNGP_FORESTS_RIVERS_OCEANS', 658), ('TAX_ETHNICITY_BLACK', 1393), \
        ('TAX_ETHNICITY_BLACK', 1408), ('TAX_ETHNICITY_BLACK', 1544), ('TAX_ETHNICITY_BLACK', 1801), ('TAX_ETHNICITY_BLACK', 1843)
        ]


# df.select('V1Locations').show(truncate=False)
def test_create_v1_locations_array():

    assert create_v1_locations_array(first_row[9]) == [(1, 'United States', 'US', 'US', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US')]


# df.select('V2Locations').show(truncate=False)
def test_create_v2_enhanced_locations_array():

    assert create_v2_enhanced_locations_array(first_row[10]) == [(1, 'Americans', 'US', 'US', '', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 224), \
                                                                (1, 'American', 'US', 'US', '', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 931), \
                                                                (1, 'American', 'US', 'US', '', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 968), \
                                                                (1, 'American', 'US', 'US', '', round(Decimal(39.828175), 6), round(Decimal(-98.5795), 4), 'US', 1019)] 


# df.select('V1Persons').show(truncate=False)
def test_create_v1_persons_array():

    assert create_v1_persons_array(first_row[11]) == Row(['arielle sheftall', 'sherrod brown', 'brown d-ohio'])


# df.select('V2Persons').show(truncate=False)
def test_create_v2_enhanced_persons():

   assert create_v2_enhanced_persons(first_row[12]) == [('Arielle Sheftall', 1587), ('Sherrod Brown', 74)] 


# df.select('V1Orgs').show(truncate=False)
def test_create_v1_orgs():

    assert create_v1_orgs(first_row[13]) == Row(['american academy of pediatrics', 'american hospital association', \
                                                'association of american medical colleges', 'academy of child'])


# df.select('V2Orgs').show(truncate=False)
def test_create_v2_enhanced_orgs():

    assert create_v2_enhanced_orgs(first_row[14]) == [('American Academy Of Pediatrics', 953), ('American Hospital Association', 989), \
                                                    ('Association Of American Medical Colleges', 1036), ('Academy Of Child', 2599)]


# df.select('V15tone').show(truncate=False)
def test_create_v15_tone():

    assert create_v15_tone(first_row[15]) == (-6.14406779661017, 1.69491525423729, 7.83898305084746, 9.53389830508475, 27.5423728813559, 2.11864406779661, 432)


# df.select('V21EnhancedDates').show(truncate=False)
def test_create_v21_enhanced_dates():

    assert create_v21_enhanced_dates(first_row[16]) == [(1, 0, 0, 1999, 1965)]


# df.select('V2GCAM').show(1, truncate=False)
def test_create_v2_gcam():

    assert create_v2_gcam(first_row[17]) == [
        ('wc', 432.0), ('c12.1', 44.0), ('c12.10', 73.0), ('c12.12', 35.0), ('c12.13', 13.0), ('c12.14', 27.0), ('c12.3', 22.0), ('c12.4', 10.0), \
        ('c12.5', 14.0), ('c12.7', 55.0), ('c12.8', 22.0), ('c12.9', 39.0), ('c13.12', 1.0), ('c13.2', 1.0), ('c14.1', 34.0), ('c14.10', 23.0), \
        ('c14.11', 75.0), ('c14.2', 40.0), ('c14.3', 40.0), ('c14.4', 7.0), ('c14.5', 82.0), ('c14.6', 2.0), ('c14.7', 10.0), ('c14.8', 3.0), \
        ('c14.9', 8.0), ('c15.10', 3.0), ('c15.102', 1.0), ('c15.103', 1.0), ('c15.110', 1.0), ('c15.112', 1.0), ('c15.116', 2.0), ('c15.131', 1.0), \
        ('c15.132', 1.0), ('c15.137', 1.0), ('c15.143', 1.0), ('c15.148', 7.0), ('c15.149', 2.0), ('c15.15', 5.0), ('c15.152', 1.0), ('c15.154', 1.0), \
        ('c15.159', 1.0), ('c15.168', 1.0), ('c15.171', 2.0), ('c15.172', 1.0), ('c15.173', 1.0), ('c15.176', 2.0), ('c15.178', 2.0), ('c15.18', 2.0), \
        ('c15.197', 3.0), ('c15.198', 2.0), ('c15.20', 2.0), ('c15.201', 4.0), ('c15.202', 1.0), ('c15.212', 1.0), ('c15.217', 2.0), ('c15.22', 2.0), \
        ('c15.227', 1.0), ('c15.231', 2.0), ('c15.24', 1.0), ('c15.241', 6.0), ('c15.248', 1.0), ('c15.251', 2.0), ('c15.252', 4.0), ('c15.255', 1.0), \
        ('c15.256', 2.0), ('c15.257', 1.0), ('c15.26', 1.0), ('c15.27', 1.0), ('c15.270', 2.0), ('c15.3', 2.0), ('c15.4', 1.0), ('c15.42', 1.0), \
        ('c15.48', 2.0), ('c15.53', 2.0), ('c15.57', 2.0), ('c15.7', 2.0), ('c15.71', 1.0), ('c15.72', 1.0), ('c15.83', 1.0), ('c15.86', 1.0), \
        ('c15.9', 1.0), ('c16.1', 1.0), ('c16.100', 3.0), ('c16.101', 3.0), ('c16.103', 2.0), ('c16.105', 2.0), ('c16.106', 15.0), ('c16.109', 29.0), \
        ('c16.11', 3.0), ('c16.110', 83.0), ('c16.111', 4.0), ('c16.113', 1.0), ('c16.114', 24.0), ('c16.115', 3.0), ('c16.116', 12.0), ('c16.117', 17.0), \
        ('c16.118', 27.0), ('c16.12', 38.0), ('c16.120', 27.0), ('c16.121', 27.0), ('c16.122', 3.0), ('c16.123', 1.0), ('c16.124', 4.0), ('c16.125', 44.0), \
        ('c16.126', 38.0), ('c16.127', 32.0), ('c16.128', 16.0), ('c16.129', 74.0), ('c16.130', 5.0), ('c16.131', 15.0), ('c16.132', 1.0), ('c16.134', 47.0), \
        ('c16.137', 1.0), ('c16.138', 25.0), ('c16.139', 13.0), ('c16.140', 22.0), ('c16.145', 46.0), ('c16.146', 32.0), ('c16.147', 5.0), ('c16.149', 1.0), \
        ('c16.15', 1.0), ('c16.152', 7.0), ('c16.153', 18.0), ('c16.155', 5.0), ('c16.156', 1.0), ('c16.157', 16.0), ('c16.158', 3.0), ('c16.159', 39.0), \
        ('c16.16', 5.0), ('c16.161', 21.0), ('c16.162', 9.0), ('c16.163', 18.0), ('c16.164', 9.0), ('c16.165', 2.0), ('c16.17', 1.0), ('c16.19', 7.0), \
        ('c16.2', 20.0), ('c16.21', 3.0), ('c16.22', 16.0), ('c16.23', 3.0), ('c16.24', 5.0), ('c16.26', 69.0), ('c16.27', 6.0), ('c16.29', 3.0), \
        ('c16.3', 8.0), ('c16.31', 48.0), ('c16.32', 8.0), ('c16.33', 43.0), ('c16.34', 7.0), ('c16.35', 31.0), ('c16.36', 15.0), ('c16.37', 48.0), \
        ('c16.38', 14.0), ('c16.39', 1.0), ('c16.4', 31.0), ('c16.41', 20.0), ('c16.42', 3.0), ('c16.45', 14.0), ('c16.46', 1.0), ('c16.47', 58.0), \
        ('c16.48', 5.0), ('c16.49', 1.0), ('c16.50', 9.0), ('c16.51', 4.0), ('c16.52', 13.0), ('c16.53', 6.0), ('c16.54', 3.0), ('c16.56', 7.0), \
        ('c16.57', 241.0), ('c16.58', 38.0), ('c16.59', 1.0), ('c16.6', 74.0), ('c16.60', 4.0), ('c16.61', 1.0), ('c16.62', 14.0), ('c16.63', 12.0), \
        ('c16.64', 5.0), ('c16.65', 10.0), ('c16.66', 8.0), ('c16.68', 29.0), ('c16.69', 13.0), ('c16.7', 23.0), ('c16.70', 12.0), ('c16.71', 2.0), \
        ('c16.72', 4.0), ('c16.73', 4.0), ('c16.74', 5.0), ('c16.75', 17.0), ('c16.76', 4.0), ('c16.77', 2.0), ('c16.78', 7.0), ('c16.79', 2.0), \
        ('c16.80', 1.0), ('c16.81', 7.0), ('c16.83', 1.0), ('c16.84', 15.0), ('c16.85', 3.0), ('c16.87', 73.0), ('c16.88', 66.0), ('c16.89', 13.0), \
        ('c16.90', 12.0), ('c16.91', 13.0), ('c16.92', 60.0), ('c16.93', 2.0), ('c16.94', 24.0), ('c16.95', 24.0), ('c16.96', 11.0), ('c16.97', 4.0), \
        ('c16.98', 39.0), ('c16.99', 1.0), ('c17.1', 134.0), ('c17.10', 55.0), ('c17.11', 48.0), ('c17.12', 20.0), ('c17.13', 6.0), ('c17.14', 6.0), \
        ('c17.15', 31.0), ('c17.16', 5.0), ('c17.17', 1.0), ('c17.18', 5.0), ('c17.19', 43.0), ('c17.2', 9.0), ('c17.20', 5.0), ('c17.21', 3.0), \
        ('c17.22', 11.0), ('c17.23', 8.0), ('c17.24', 36.0), ('c17.25', 3.0), ('c17.26', 1.0), ('c17.27', 65.0), ('c17.28', 9.0), ('c17.29', 19.0), \
        ('c17.3', 1.0), ('c17.30', 20.0), ('c17.31', 45.0), ('c17.32', 29.0), ('c17.33', 34.0), ('c17.34', 10.0), ('c17.35', 9.0), ('c17.36', 22.0), \
        ('c17.37', 24.0), ('c17.38', 6.0), ('c17.39', 13.0), ('c17.4', 105.0), ('c17.40', 16.0), ('c17.41', 22.0), ('c17.42', 36.0), ('c17.43', 30.0), \
        ('c17.5', 111.0), ('c17.6', 7.0), ('c17.7', 54.0), ('c17.8', 77.0), ('c17.9', 5.0), ('c18.1', 1.0), ('c18.100', 1.0), ('c18.13', 1.0), ('c18.139', 1.0), \
        ('c18.147', 3.0), ('c18.149', 9.0), ('c18.180', 10.0), ('c18.190', 3.0), ('c18.193', 11.0), ('c18.270', 13.0), ('c18.298', 4.0), ('c18.34', 13.0), \
        ('c18.342', 7.0), ('c18.78', 1.0), ('c2.1', 25.0), ('c2.10', 2.0), ('c2.100', 2.0), ('c2.101', 9.0), ('c2.102', 11.0), ('c2.104', 86.0), ('c2.107', 2.0), \
        ('c2.108', 1.0), ('c2.109', 1.0), ('c2.11', 11.0), ('c2.110', 4.0), ('c2.111', 1.0), ('c2.112', 13.0), ('c2.113', 7.0), ('c2.114', 29.0), ('c2.115', 1.0), \
        ('c2.116', 17.0), ('c2.117', 1.0), ('c2.118', 17.0), ('c2.119', 134.0), ('c2.12', 21.0), ('c2.120', 1.0), ('c2.121', 27.0), ('c2.122', 13.0), ('c2.123', 2.0), \
        ('c2.124', 7.0), ('c2.125', 22.0), ('c2.126', 20.0), ('c2.127', 48.0), ('c2.128', 9.0), ('c2.129', 19.0), ('c2.130', 2.0), ('c2.131', 2.0), ('c2.132', 3.0), \
        ('c2.133', 1.0), ('c2.134', 3.0), ('c2.135', 1.0), ('c2.136', 2.0), ('c2.137', 1.0), ('c2.138', 1.0), ('c2.139', 3.0), ('c2.14', 48.0), ('c2.140', 1.0), \
        ('c2.141', 7.0), ('c2.142', 10.0), ('c2.143', 31.0), ('c2.144', 10.0), ('c2.145', 5.0), ('c2.146', 5.0), ('c2.147', 66.0), ('c2.148', 41.0), ('c2.149', 1.0), \
        ('c2.15', 26.0), ('c2.150', 6.0), ('c2.151', 1.0), ('c2.152', 2.0), ('c2.153', 22.0), ('c2.154', 7.0), ('c2.155', 48.0), ('c2.156', 23.0), ('c2.157', 42.0), \
        ('c2.158', 39.0), ('c2.159', 2.0), ('c2.160', 18.0), ('c2.162', 9.0), ('c2.163', 2.0), ('c2.166', 11.0), ('c2.169', 12.0), ('c2.17', 5.0), ('c2.170', 12.0), \
        ('c2.171', 8.0), ('c2.173', 7.0), ('c2.175', 1.0), ('c2.176', 3.0), ('c2.177', 36.0), ('c2.179', 32.0), ('c2.18', 13.0), ('c2.180', 14.0), ('c2.181', 16.0), \
        ('c2.183', 16.0), ('c2.185', 115.0), ('c2.186', 8.0), ('c2.187', 35.0), ('c2.19', 2.0), ('c2.191', 7.0), ('c2.192', 18.0), ('c2.193', 22.0), ('c2.195', 49.0), \
        ('c2.196', 18.0), ('c2.197', 14.0), ('c2.198', 29.0), ('c2.199', 8.0), ('c2.2', 2.0), ('c2.20', 1.0), ('c2.200', 7.0), ('c2.201', 2.0), ('c2.203', 26.0), \
        ('c2.204', 36.0), ('c2.205', 7.0), ('c2.206', 4.0), ('c2.207', 7.0), ('c2.209', 11.0), ('c2.210', 49.0), ('c2.211', 1.0), ('c2.213', 28.0), ('c2.214', 20.0), \
        ('c2.216', 6.0), ('c2.217', 30.0), ('c2.218', 3.0), ('c2.219', 9.0), ('c2.220', 46.0), ('c2.221', 4.0), ('c2.223', 5.0), ('c2.225', 19.0), ('c2.226', 12.0), \
        ('c2.227', 4.0), ('c2.228', 2.0), ('c2.23', 9.0), ('c2.24', 13.0), ('c2.25', 22.0), ('c2.26', 28.0), ('c2.27', 28.0), ('c2.28', 11.0), ('c2.30', 14.0), \
        ('c2.31', 18.0), ('c2.32', 1.0), ('c2.33', 8.0), ('c2.34', 33.0), ('c2.35', 15.0), ('c2.36', 12.0), ('c2.37', 15.0), ('c2.39', 61.0), ('c2.4', 2.0), \
        ('c2.42', 2.0), ('c2.43', 1.0), ('c2.44', 17.0), ('c2.45', 11.0), ('c2.46', 58.0), ('c2.47', 2.0), ('c2.48', 15.0), ('c2.49', 1.0), ('c2.50', 7.0), \
        ('c2.52', 39.0), ('c2.53', 2.0), ('c2.54', 43.0), ('c2.55', 1.0), ('c2.56', 1.0), ('c2.57', 7.0), ('c2.58', 8.0), ('c2.59', 1.0), ('c2.6', 2.0), \
        ('c2.61', 5.0), ('c2.62', 15.0), ('c2.64', 9.0), ('c2.65', 2.0), ('c2.66', 3.0), ('c2.68', 2.0), ('c2.69', 1.0), ('c2.70', 3.0), ('c2.71', 2.0), \
        ('c2.73', 7.0), ('c2.74', 2.0), ('c2.75', 83.0), ('c2.76', 317.0), ('c2.77', 46.0), ('c2.78', 62.0), ('c2.79', 8.0), ('c2.80', 62.0), ('c2.81', 5.0), \
        ('c2.82', 21.0), ('c2.83', 9.0), ('c2.84', 9.0), ('c2.85', 1.0), ('c2.86', 16.0), ('c2.87', 8.0), ('c2.88', 20.0), ('c2.89', 27.0), ('c2.9', 1.0), \
        ('c2.90', 11.0), ('c2.93', 16.0), ('c2.94', 2.0), ('c2.95', 58.0), ('c2.96', 2.0), ('c2.97', 2.0), ('c2.98', 35.0), ('c2.99', 3.0), ('c25.1', 4.0), \
        ('c25.11', 1.0), ('c25.5', 2.0), ('c3.1', 38.0), ('c3.2', 29.0), ('c35.1', 2.0), ('c35.11', 4.0), ('c35.14', 4.0), ('c35.15', 8.0), ('c35.2', 2.0), \
        ('c35.20', 11.0), ('c35.25', 1.0), ('c35.28', 1.0), ('c35.3', 1.0), ('c35.31', 17.0), ('c35.32', 12.0), ('c35.33', 13.0), ('c35.4', 2.0), ('c35.5', 5.0), \
        ('c35.7', 1.0), ('c39.1', 1.0), ('c39.13', 1.0), ('c39.14', 1.0), ('c39.17', 4.0), ('c39.18', 1.0), ('c39.2', 8.0), ('c39.20', 1.0), ('c39.24', 1.0), \
        ('c39.25', 10.0), ('c39.26', 1.0), ('c39.27', 1.0), ('c39.28', 1.0), ('c39.3', 23.0), ('c39.36', 2.0), ('c39.37', 21.0), ('c39.39', 3.0), ('c39.4', 22.0), \
        ('c39.40', 1.0), ('c39.41', 5.0), ('c39.5', 12.0), ('c39.6', 1.0), ('c4.1', 1.0), ('c4.13', 4.0), ('c4.23', 13.0), ('c4.3', 9.0), ('c40.5', 3.0), ('c41.1', 32.0), \
        ('c42.1', 165.0), ('c5.1', 1.0), ('c5.10', 26.0), ('c5.11', 14.0), ('c5.12', 67.0), ('c5.15', 11.0), ('c5.16', 1.0), ('c5.17', 12.0), ('c5.18', 3.0), ('c5.19', 5.0), \
        ('c5.2', 19.0), ('c5.20', 16.0), ('c5.21', 24.0), ('c5.22', 3.0), ('c5.23', 10.0), ('c5.24', 9.0), ('c5.25', 6.0), ('c5.26', 4.0), ('c5.27', 2.0), ('c5.28', 12.0), \
        ('c5.29', 11.0), ('c5.30', 55.0), ('c5.31', 3.0), ('c5.32', 2.0), ('c5.33', 4.0), ('c5.34', 15.0), ('c5.35', 10.0), ('c5.36', 25.0), ('c5.37', 11.0), ('c5.4', 4.0), \
        ('c5.40', 47.0), ('c5.42', 2.0), ('c5.43', 11.0), ('c5.44', 3.0), ('c5.45', 11.0), ('c5.46', 67.0), ('c5.47', 8.0), ('c5.48', 4.0), ('c5.49', 62.0), ('c5.50', 66.0), \
        ('c5.51', 41.0), ('c5.52', 80.0), ('c5.53', 32.0), ('c5.54', 20.0), ('c5.55', 8.0), ('c5.56', 4.0), ('c5.57', 2.0), ('c5.58', 7.0), ('c5.6', 14.0), ('c5.60', 21.0), \
        ('c5.61', 41.0), ('c5.62', 193.0), ('c5.7', 9.0), ('c5.8', 21.0), ('c5.9', 26.0), ('c6.1', 3.0), ('c6.2', 2.0), ('c6.3', 1.0), ('c6.4', 22.0), ('c6.5', 4.0), ('c6.6', 3.0), \
        ('c7.1', 50.0), ('c7.2', 27.0), ('c8.1', 3.0), ('c8.10', 5.0), ('c8.11', 1.0), ('c8.13', 3.0), ('c8.2', 3.0), ('c8.20', 1.0), ('c8.23', 8.0), ('c8.27', 2.0), ('c8.28', 7.0), \
        ('c8.3', 4.0), ('c8.35', 1.0), ('c8.36', 11.0), ('c8.37', 21.0), ('c8.38', 8.0), ('c8.39', 1.0), ('c8.4', 14.0), ('c8.40', 2.0), ('c8.41', 4.0), ('c8.42', 13.0), ('c8.43', 20.0), \
        ('c8.5', 1.0), ('c8.6', 1.0), ('c9.1', 18.0), ('c9.10', 3.0), ('c9.1000', 1.0), ('c9.1011', 5.0), ('c9.1012', 2.0), ('c9.1018', 7.0), ('c9.1030', 2.0), ('c9.1036', 2.0), \
        ('c9.1038', 2.0), ('c9.1039', 1.0), ('c9.104', 1.0), ('c9.1040', 1.0), ('c9.107', 5.0), ('c9.109', 6.0), ('c9.11', 2.0), ('c9.110', 2.0), ('c9.111', 7.0), ('c9.113', 2.0), \
        ('c9.116', 1.0), ('c9.118', 5.0), ('c9.119', 2.0), ('c9.12', 1.0), ('c9.122', 5.0), ('c9.123', 1.0), ('c9.124', 2.0), ('c9.125', 4.0), ('c9.127', 2.0), ('c9.128', 27.0), \
        ('c9.129', 10.0), ('c9.130', 8.0), ('c9.133', 11.0), ('c9.134', 5.0), ('c9.135', 14.0), ('c9.137', 3.0), ('c9.138', 1.0), ('c9.141', 2.0), ('c9.142', 1.0), ('c9.143', 5.0), \
        ('c9.145', 1.0), ('c9.148', 1.0), ('c9.149', 3.0), ('c9.15', 3.0), ('c9.151', 6.0), ('c9.157', 2.0), ('c9.158', 11.0), ('c9.159', 2.0), ('c9.160', 6.0), ('c9.161', 1.0), \
        ('c9.162', 10.0), ('c9.163', 3.0), ('c9.164', 2.0), ('c9.165', 1.0), ('c9.167', 10.0), ('c9.168', 7.0), ('c9.169', 2.0), ('c9.174', 4.0), ('c9.175', 3.0), ('c9.177', 6.0), \
        ('c9.178', 1.0), ('c9.179', 1.0), ('c9.18', 2.0), ('c9.180', 2.0), ('c9.182', 7.0), ('c9.184', 9.0), ('c9.188', 5.0), ('c9.195', 1.0), ('c9.196', 2.0), ('c9.197', 4.0), \
        ('c9.2', 4.0), ('c9.20', 2.0), ('c9.200', 3.0), ('c9.201', 1.0), ('c9.203', 3.0), ('c9.205', 1.0), ('c9.207', 1.0), ('c9.208', 1.0), ('c9.212', 2.0), ('c9.215', 2.0), \
        ('c9.220', 1.0), ('c9.224', 2.0), ('c9.227', 1.0), ('c9.229', 1.0), ('c9.23', 2.0), ('c9.231', 2.0), ('c9.232', 1.0), ('c9.233', 3.0), ('c9.235', 1.0), ('c9.238', 3.0), \
        ('c9.24', 1.0), ('c9.245', 1.0), ('c9.25', 2.0), ('c9.250', 2.0), ('c9.253', 2.0), ('c9.260', 2.0), ('c9.263', 1.0), ('c9.265', 1.0), ('c9.267', 1.0), ('c9.27', 2.0), \
        ('c9.270', 1.0), ('c9.274', 1.0), ('c9.275', 1.0), ('c9.281', 1.0), ('c9.284', 1.0), ('c9.288', 2.0), ('c9.289', 1.0), ('c9.290', 4.0), ('c9.291', 2.0), ('c9.292', 1.0), \
        ('c9.3', 15.0), ('c9.30', 2.0), ('c9.302', 3.0), ('c9.306', 1.0), ('c9.307', 1.0), ('c9.310', 2.0), ('c9.313', 1.0), ('c9.315', 1.0), ('c9.317', 3.0), ('c9.32', 3.0), \
        ('c9.324', 1.0), ('c9.326', 1.0), ('c9.329', 1.0), ('c9.33', 8.0), ('c9.330', 3.0), ('c9.335', 3.0), ('c9.34', 6.0), ('c9.35', 4.0), ('c9.353', 1.0), ('c9.358', 1.0), \
        ('c9.359', 1.0), ('c9.360', 1.0), ('c9.37', 5.0), ('c9.370', 1.0), ('c9.371', 6.0), ('c9.372', 15.0), ('c9.374', 1.0), ('c9.378', 1.0), ('c9.383', 1.0), ('c9.384', 10.0), \
        ('c9.385', 8.0), ('c9.387', 1.0), ('c9.389', 1.0), ('c9.39', 8.0), ('c9.391', 1.0), ('c9.393', 1.0), ('c9.395', 1.0), ('c9.402', 2.0), ('c9.405', 2.0), ('c9.419', 2.0), \
        ('c9.420', 1.0), ('c9.423', 1.0), ('c9.427', 1.0), ('c9.429', 1.0), ('c9.43', 1.0), ('c9.430', 2.0), ('c9.432', 2.0), ('c9.433', 1.0), ('c9.438', 5.0), ('c9.439', 2.0), \
        ('c9.44', 4.0), ('c9.440', 2.0), ('c9.446', 3.0), ('c9.447', 3.0), ('c9.448', 1.0), ('c9.449', 7.0), ('c9.45', 1.0), ('c9.450', 7.0), ('c9.451', 7.0), ('c9.452', 3.0), \
        ('c9.454', 2.0), ('c9.455', 2.0), ('c9.456', 2.0), ('c9.457', 2.0), ('c9.458', 3.0), ('c9.459', 8.0), ('c9.46', 4.0), ('c9.463', 2.0), ('c9.466', 4.0), ('c9.467', 2.0), \
        ('c9.468', 2.0), ('c9.47', 3.0), ('c9.470', 1.0), ('c9.474', 5.0), ('c9.476', 7.0), ('c9.477', 3.0), ('c9.478', 4.0), ('c9.479', 9.0), ('c9.48', 4.0), ('c9.480', 13.0), \
        ('c9.481', 3.0), ('c9.483', 2.0), ('c9.485', 4.0), ('c9.487', 1.0), ('c9.488', 1.0), ('c9.489', 3.0), ('c9.49', 1.0), ('c9.491', 3.0), ('c9.492', 2.0), ('c9.494', 4.0), \
        ('c9.496', 9.0), ('c9.497', 1.0), ('c9.498', 8.0), ('c9.499', 3.0), ('c9.5', 1.0), ('c9.501', 5.0), ('c9.502', 1.0), ('c9.503', 1.0), ('c9.504', 5.0), ('c9.507', 5.0), \
        ('c9.509', 6.0), ('c9.511', 8.0), ('c9.513', 12.0), ('c9.517', 1.0), ('c9.519', 6.0), ('c9.521', 1.0), ('c9.522', 8.0), ('c9.523', 4.0), ('c9.524', 4.0), ('c9.53', 1.0), \
        ('c9.537', 1.0), ('c9.538', 2.0), ('c9.540', 5.0), ('c9.542', 2.0), ('c9.545', 2.0), ('c9.546', 2.0), ('c9.549', 1.0), ('c9.55', 7.0), ('c9.551', 4.0), ('c9.556', 5.0), \
        ('c9.557', 6.0), ('c9.559', 2.0), ('c9.560', 5.0), ('c9.561', 1.0), ('c9.562', 4.0), ('c9.564', 3.0), ('c9.567', 3.0), ('c9.568', 1.0), ('c9.569', 2.0), ('c9.57', 3.0), \
        ('c9.570', 5.0), ('c9.571', 2.0), ('c9.574', 2.0), ('c9.575', 5.0), ('c9.576', 1.0), ('c9.579', 16.0), ('c9.581', 9.0), ('c9.583', 1.0), ('c9.585', 2.0), ('c9.588', 5.0), \
        ('c9.589', 3.0), ('c9.59', 2.0), ('c9.590', 3.0), ('c9.591', 2.0), ('c9.600', 10.0), ('c9.604', 1.0), ('c9.608', 1.0), ('c9.61', 1.0), ('c9.613', 1.0), ('c9.616', 3.0), \
        ('c9.617', 1.0), ('c9.618', 4.0), ('c9.619', 1.0), ('c9.62', 1.0), ('c9.620', 1.0), ('c9.622', 2.0), ('c9.624', 5.0), ('c9.625', 4.0), ('c9.626', 1.0), ('c9.627', 3.0), \
        ('c9.629', 2.0), ('c9.632', 2.0), ('c9.635', 2.0), ('c9.638', 1.0), ('c9.64', 2.0), ('c9.640', 7.0), ('c9.642', 13.0), ('c9.645', 2.0), ('c9.647', 3.0), ('c9.648', 13.0), \
        ('c9.649', 6.0), ('c9.653', 23.0), ('c9.654', 7.0), ('c9.655', 2.0), ('c9.658', 3.0), ('c9.659', 3.0), ('c9.66', 6.0), ('c9.660', 10.0), ('c9.661', 2.0), ('c9.663', 2.0), \
        ('c9.664', 1.0), ('c9.665', 4.0), ('c9.666', 1.0), ('c9.667', 3.0), ('c9.668', 2.0), ('c9.669', 4.0), ('c9.67', 1.0), ('c9.670', 10.0), ('c9.671', 8.0), ('c9.672', 1.0), \
        ('c9.673', 5.0), ('c9.674', 2.0), ('c9.676', 3.0), ('c9.677', 4.0), ('c9.678', 1.0), ('c9.679', 3.0), ('c9.680', 2.0), ('c9.682', 3.0), ('c9.683', 10.0), ('c9.684', 3.0), \
        ('c9.686', 5.0), ('c9.687', 7.0), ('c9.690', 11.0), ('c9.691', 1.0), ('c9.692', 7.0), ('c9.693', 7.0), ('c9.694', 1.0), ('c9.696', 2.0), ('c9.697', 2.0), ('c9.698', 4.0), \
        ('c9.7', 1.0), ('c9.70', 8.0), ('c9.701', 13.0), ('c9.704', 8.0), ('c9.705', 1.0), ('c9.708', 5.0), ('c9.71', 3.0), ('c9.710', 3.0), ('c9.712', 2.0), ('c9.713', 1.0), \
        ('c9.719', 1.0), ('c9.72', 1.0), ('c9.720', 2.0), ('c9.722', 1.0), ('c9.723', 3.0), ('c9.724', 3.0), ('c9.725', 2.0), ('c9.726', 23.0), ('c9.730', 16.0), ('c9.732', 1.0), \
        ('c9.734', 4.0), ('c9.735', 7.0), ('c9.736', 3.0), ('c9.737', 4.0), ('c9.739', 1.0), ('c9.74', 1.0), ('c9.740', 6.0), ('c9.741', 3.0), ('c9.745', 3.0), ('c9.746', 2.0), \
        ('c9.748', 10.0), ('c9.752', 2.0), ('c9.755', 3.0), ('c9.757', 4.0), ('c9.759', 1.0), ('c9.76', 8.0), ('c9.760', 1.0), ('c9.762', 13.0), ('c9.763', 2.0), ('c9.765', 1.0), \
        ('c9.767', 27.0), ('c9.768', 2.0), ('c9.770', 2.0), ('c9.771', 4.0), ('c9.772', 1.0), ('c9.774', 3.0), ('c9.776', 2.0), ('c9.778', 1.0), ('c9.78', 1.0), ('c9.781', 3.0), \
        ('c9.788', 1.0), ('c9.789', 2.0), ('c9.790', 3.0), ('c9.792', 2.0), ('c9.793', 1.0), ('c9.795', 1.0), ('c9.8', 2.0), ('c9.80', 1.0), ('c9.802', 5.0), ('c9.806', 4.0), \
        ('c9.807', 2.0), ('c9.808', 7.0), ('c9.812', 4.0), ('c9.816', 4.0), ('c9.818', 1.0), ('c9.82', 5.0), ('c9.821', 3.0), ('c9.823', 1.0), ('c9.824', 1.0), ('c9.826', 1.0), \
        ('c9.83', 8.0), ('c9.831', 1.0), ('c9.833', 3.0), ('c9.834', 4.0), ('c9.837', 3.0), ('c9.838', 1.0), ('c9.840', 3.0), ('c9.845', 4.0), ('c9.846', 4.0), ('c9.853', 3.0), \
        ('c9.855', 1.0), ('c9.857', 1.0), ('c9.858', 2.0), ('c9.860', 10.0), ('c9.861', 2.0), ('c9.862', 1.0), ('c9.863', 4.0), ('c9.864', 13.0), ('c9.865', 4.0), ('c9.867', 3.0), \
        ('c9.868', 13.0), ('c9.870', 2.0), ('c9.872', 1.0), ('c9.873', 2.0), ('c9.874', 1.0), ('c9.877', 2.0), ('c9.878', 2.0), ('c9.879', 1.0), ('c9.88', 1.0), ('c9.882', 11.0), \
        ('c9.883', 7.0), ('c9.884', 3.0), ('c9.889', 1.0), ('c9.89', 3.0), ('c9.890', 1.0), ('c9.893', 1.0), ('c9.896', 4.0), ('c9.897', 4.0), ('c9.898', 4.0), ('c9.899', 1.0), \
        ('c9.90', 4.0), ('c9.900', 1.0), ('c9.901', 3.0), ('c9.902', 4.0), ('c9.903', 3.0), ('c9.904', 3.0), ('c9.908', 9.0), ('c9.911', 5.0), ('c9.915', 2.0), ('c9.916', 1.0), \
        ('c9.920', 1.0), ('c9.923', 4.0), ('c9.926', 8.0), ('c9.930', 8.0), ('c9.935', 11.0), ('c9.938', 2.0), ('c9.940', 1.0), ('c9.942', 1.0), ('c9.945', 3.0), ('c9.946', 6.0), \
        ('c9.948', 1.0), ('c9.95', 2.0), ('c9.953', 1.0), ('c9.955', 3.0), ('c9.96', 5.0), ('c9.963', 3.0), ('c9.964', 2.0), ('c9.965', 1.0), ('c9.966', 6.0), ('c9.969', 1.0), \
        ('c9.972', 8.0), ('c9.973', 1.0), ('c9.975', 1.0), ('c9.976', 5.0), ('c9.977', 5.0), ('c9.978', 8.0), ('c9.98', 2.0), ('c9.980', 1.0), ('c9.981', 1.0), ('c9.984', 4.0), \
        ('c9.985', 3.0), ('c9.986', 8.0), ('c9.987', 1.0), ('c9.992', 1.0), ('v10.1', 0.2864453125), ('v10.2', 0.265868019869818), ('v11.1', -0.00909062318840579), \
        ('v19.1', 5.11492957746479), ('v19.2', 5.39), ('v19.3', 4.89239436619718), ('v19.4', 5.13014084507042), ('v19.5', 5.20225352112676), ('v19.6', 5.20718309859155), \
        ('v19.7', 5.11971830985916), ('v19.8', 5.56718309859155), ('v19.9', 4.6343661971831), ('v20.1', 0.325), ('v20.10', -0.589230769230769), ('v20.11', 0.492857142857143), \
        ('v20.12', -0.5725), ('v20.13', 0.392060606060606), ('v20.14', -0.546052631578947), ('v20.15', 0.336814814814815), ('v20.16', -0.53125), ('v20.2', -0.357), \
        ('v20.3', 0.325), ('v20.4', -0.339166666666667), ('v20.5', 0.325), ('v20.6', -0.545555555555556), ('v20.7', 0.325), ('v20.8', -0.582727272727273), ('v20.9', 0.475), \
        ('v21.1', 5.32725888324873), ('v26.1', -0.816), ('v42.10', -0.0928580947575758), ('v42.11', -0.105255434369697), ('v42.2', 0.143426244818182), ('v42.3', 0.0989954048121212), \
        ('v42.4', 0.0975243302060606), ('v42.5', 0.102464749145455), ('v42.6', 0.0962527494424242), ('v42.7', -0.125047707242424), ('v42.8', -0.0939613242060606), ('v42.9', -0.0576108369636364)
        ]


# df.select('V21ShareImg').show(truncate=False)
def test_create_v21_share_img():

    assert create_v21_share_img(first_row[18]) == Row('https://bloximages.chicago2.vip.townnews.com/bryantimes.com/content/tncms/custom/image/45f9f6d2-e0e6-11e7-954d-4f5f4b273a47.jpg')


# df.select('V21RelImg').show(truncate=False)
# Testing with row 19 as it's the first row with a value for this column
def test_create_v21_rel_img():

    assert create_v21_rel_img(first_row[19]) == None
    assert create_v21_rel_img(row_list[19][19]) == Row(['https://www.thewrap.com/wp-content/uploads/2021/10/netflix-walkout-image-9-1024x576.jpg', \
                                                        'https://www.thewrap.com/wp-content/uploads/2021/10/netflix-walkout-image-13-1024x576.jpg'])


# df.select('V21SocImage').show(truncate=False)
# Testing with row 27 as it's the first row with a value for this column
def test_create_v21_soc_img():

    assert create_v21_soc_img(first_row[20]) == None
    assert create_v21_soc_img(row_list[27][20]) == Row(['https://pic.twitter.com/pdIDCBv9au'])


# df.select('V21SocVideo').show(truncate=False) 
def test_create_v21_soc_vid():

    assert create_v21_soc_vid(first_row[21]) == Row(['https://youtube.com/user/bryantimes'])


# df.select('V21Quotations').show(truncate=False)
# Testing with row 2 as it's the first row with a value for this column
def test_create_v21_quotes_array():

    assert create_v21_quotes_array(first_row[22]) == None
    assert create_v21_quotes_array(row_list[2][22]) == [(359, 114, '', 'On Thursday , the Bulgarian side of the project sealed the final deal with the Chinese private company Great Wall')]

 
# df.select('V21AllNames').show(truncate=False)
def test_create_v21_all_names():

    assert create_v21_all_names(first_row[23]) == [('Sherrod Brown', 78), ('Child Suicide Prevention', 132), ('Lethal Means Safety', 156), \
                                                    ('Democratic Congresswoman Lauren Underwood', 514), ('Child Suicide Prevention', 563), \
                                                    ('Lethal Means Safety', 587), ('American Academy', 992), ('American Hospital Association', 1045), \
                                                    ('American Medical', 1085), ('Arielle Sheftall', 1661), ('Suicide Prevention', 1726), \
                                                    ('Abigail Wexner Research Institute', 1780), ('Nationwide Children', 1803), ('Adolescent Psychology', 2770)]


# df.select('V21Amounts').show(truncate=False)
def test_create_v21_amounts():

    assert create_v21_amounts(first_row[24]) == [(4.0, 'leading cause of death', 1602)]


# df.select('V21TransInfo').show(truncate=False)
def test_create_v21_trans_info():

    assert create_v21_trans_info(first_row[25]) == (None, None)


# df.select('V2ExtrasXML').show(truncate=False)
def test_create_v2_extras_xml():

    assert create_v2_extras_xml(first_row[26]) == ('Brown supporting child suicide prevention bill', 'Lucas Bechtol lbechtol@bryantimes.com;ltbechtol', \
                                                    None, None, None, datetime.datetime.strptime('20211020231500', '%Y%m%d%H%M%S'))


# df.show(1, truncate=False)
def test_gkg_parser():

    assert gkg_parser(line=str(first_row)) == (test_create_gkg_record_id(), test_create_v21_date(), test_create_v2_src_collection_id(), \
                                            test_create_v2_src_common_name(), test_create_v2_doc_id(), test_create_v1_count_array(), \
                                            test_create_v21_count_array(), test_create_v1_themes_array(), test_create_v2_enhanced_themes_array(), \
                                            test_create_v1_locations_array(), test_create_v2_enhanced_locations_array(), test_create_v1_persons_array(), \
                                            test_create_v2_enhanced_persons(), test_create_v1_orgs(), test_create_v2_enhanced_orgs(), test_create_v15_tone(), \
                                            test_create_v21_enhanced_dates(), test_create_v2_gcam(), test_create_v21_share_img(), test_create_v21_rel_img(), \
                                            test_create_v21_soc_img(), test_create_v21_soc_vid(), test_create_v21_quotes_array(), test_create_v21_all_names(), \
                                            test_create_v21_amounts(), test_create_v21_trans_info(), test_create_v2_extras_xml())