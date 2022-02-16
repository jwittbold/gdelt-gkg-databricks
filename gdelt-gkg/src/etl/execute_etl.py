import os
import sys
import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.dbutils import DBUtils
from cloudpathlib import CloudPath

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.toml_config import config
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser



# build spark
spark = SparkSession \
    .builder \
    .appName('write_transformed_gkg') \
    .getOrCreate()


#instantiate DBUtils
dbutils = DBUtils(spark)


# DBFS mount path
DBFS_MNT = config['FS']['DBFS_MNT']

# cloudpath lib prefix az://<container_name>
CLOUD_PATH = f"{config['FS']['CLOUD_PATH']}{config['AZURE']['CONTAINER']}"

# Azure ADLS Gen 2 Storage Account Path
# abfs[s]://<file_system>@<storage_account_name>.dfs.core.windows.net
ADLS_STORAGE = f"{config['AZURE']['PREFIX']}{config['AZURE']['CONTAINER']}@{config['AZURE']['STORAGE_ACC']}{config['AZURE']['SUFFIX']}"

# ETL folder paths '/gdelt/<folder>'
DOWNLOAD_PATH = config['ETL']['PATHS']['DOWNLOAD_PATH']
EXTRACT_PATH = config['ETL']['PATHS']['EXTRACT_PATH']
DOWNLOAD_METRICS = config['SCRAPER']['DOWNLOAD_METRICS']
PIPELINE_METRICS_TEMP = config['ETL']['PATHS']['PIPELINE_METRICS_TEMP']
PIPELINE_METRICS_FINAL = config['ETL']['PATHS']['PIPELINE_METRICS_FINAL']


def removeprefix(str_: str, prefix: str) -> str:
    if str(str_).startswith(prefix):
        return str(str_)[len(prefix):]
    else:
        return str(str_)[:]


class GkgBatchWriter:

    def __init__(self):
        self.config = config
        self.gkg_glob = str('')
        self.gkg_dict = dict({})
        self.period = str('')
        self.raw_input_path = str('')
        self.file_name = str('')
        self.translingual = bool()
        self.numeric_date_str = str('')
        self.date_time = str('')
        self.year = str('')
        self.month = str('')
        self.day = str('')
        self.hour = str('')
        self.minute = str()
        self.second = str('')
        self.gkg_version = str('')



    def check_processed(self):
        """
        """
        try:       
            # checks if pipeline_metrics_final folder contains non-hidden files
            if [f for f in dbutils.fs.ls(f'{DBFS_MNT}{PIPELINE_METRICS_FINAL}') if not f[0].startswith('.')] != []:

                print('Pipeline metrics FINAL exists, reading from FINAL folder')
                pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{ADLS_STORAGE}{PIPELINE_METRICS_FINAL}/*.parquet') 
                unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])
                processed_gkg_array = unique_metrics_df.select('file_name').rdd.flatMap(lambda row: row).collect()
                
                # return list of already processed files and True. (True for use when writing out batch and creating pipeline metrics final DF)
                return processed_gkg_array, True

            # checks if pipeline_metrics_temp folder contains non-hidden files
            elif [f for f in dbutils.fs.ls(f'{DBFS_MNT}{PIPELINE_METRICS_TEMP}') if not f[0].startswith('.')] != []:

                print('Pipeline metrics TEMP exists, reading from TEMP folder')
                pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{ADLS_STORAGE}{PIPELINE_METRICS_TEMP}/*.parquet') 
                unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])
                processed_gkg_array = unique_metrics_df.select('file_name').rdd.flatMap(lambda row: row).collect()
                
                # return list of already processed files and False (False for use when writing out batch and creating pipeline metrics final DF)
                return processed_gkg_array, False

            else:
                print(f'No record of processed GKG files. Processing GKG and creating pipeline metrics...')
                return [],[]

        except Exception as e:
            print(f'\nEncountered exception while checking for processed files in pipeline metrics folder:\n{e}')



    def batch_processor(self):
        """
        """

        try:
            processor = GkgBatchWriter()
               
            input_path = f'{DBFS_MNT}{DOWNLOAD_PATH}'
            processor.period = self.config['BATCH']['PERIOD']
            
            gkg_glob_list = []
            processor.gkg_glob = gkg_glob_list

            for f in dbutils.fs.ls(input_path):
                if f[0].endswith('.csv'):
                    gkg_glob_list.append(f[0])

            print(f'Checking for previously processed GKG files...')
            processed_gkg = GkgBatchWriter().check_processed()[0]

                
            for gkg_file in sorted(processor.gkg_glob):

                file_name = gkg_file.split('/')[-1]

                if file_name not in processed_gkg:

                    processor.raw_input_path = input_path
                    processor.file_name = gkg_file.split('/')[-1].rstrip('.csv')
                    processor.numeric_date_str = gkg_file.split('/')[-1].split('.')[0]
                    processor.date_time = datetime.datetime.strptime(processor.numeric_date_str, '%Y%m%d%H%M%S')
                    processor.year = gkg_file.split('/')[-1].split('.')[0][:4]
                    processor.month = gkg_file.split('/')[-1].split('.')[0][4:6]
                    processor.day = gkg_file.split('/')[-1].split('.')[0][6:8]
                    processor.hour = gkg_file.split('/')[-1].split('.')[0][8:10]
                    processor.minute = gkg_file.split('/')[-1].split('.')[0][10:12]
                    processor.second = gkg_file.split('/')[-1].split('.')[0][12:14]
                    processor.gkg_version = processor.file_name[14:]


                    if processor.period == '15min':
                        gkg_key = f'{processor.year}{processor.month}{processor.day}{processor.hour}{processor.minute}|{processor.gkg_version}'
                        etl_mode = '15min'

                    elif processor.period == 'hourly':
                        gkg_key = f'{processor.year}{processor.month}{processor.day}{processor.hour}|{processor.gkg_version}'
                        etl_mode = 'hourly'

                    elif processor.period == 'daily':
                        gkg_key = f'{processor.year}{processor.month}{processor.day}|{processor.gkg_version}'
                        etl_mode = 'daily'

                    else:
                        raise ValueError("BATCH PERIOD must be set to 'daily', 'hourly', or '15min' within config.toml file.")

                    if gkg_key not in processor.gkg_dict:

                        processor.gkg_dict[gkg_key] = ''

                    if gkg_key in processor.gkg_dict.keys():
                        pass

                    for key in processor.gkg_dict:

                        if processor.period == '15min':
                            prefix = key.split('|')[0]
                            suffix = key[13:]

                        elif processor.period == 'hourly':
                            prefix = key.split('|')[0]
                            suffix = key[11:]

                        elif processor.period == 'daily':
                            prefix = key.split('|')[0]
                            suffix = key[9:]
                        
                        else:
                            raise ValueError("BATCH PERIOD must be set to 'daily', 'hourly', or '15min' within config.toml file.")

                        if gkg_file.startswith(f'{processor.raw_input_path}/{prefix}') and suffix == processor.gkg_version:

                            # concat paths for use as large RDD
                            processor.gkg_dict[key] += f'{gkg_file},'

                else:
                    print(f'{file_name} -- file already processed, passing.')

            return processor.gkg_dict.values()
        
        except Exception as e:
            print(f'Encountered exception when processing batch:\n{e}')



    def create_pipeline_metrics(self, fs, etl_out_path, version, year, month, day):
        """
        """
        try:
            gkg_batch_df = spark.read \
                            .format('parquet') \
                            .schema(gkg_schema) \
                            .load(f'{ADLS_STORAGE}{etl_out_path}/{version}/{year}/{month}/{day}/*.parquet')
            

            gkg_metrics = gkg_batch_df \
                                .select([
                                    F.col('GkgRecordId.Date').alias('gkg_record_id_a'), \
                                    F.col('GkgRecordId.Translingual').alias('translingual_a')]) \
                                    .withColumn('etl_timestamp', F.lit(datetime.datetime.now().isoformat(timespec='seconds', sep=' ')) \
                                    .cast(TimestampType())) \
                                .groupBy('gkg_record_id_a', 'translingual_a', 'etl_timestamp') \
                                .count() \
                                .withColumnRenamed('count', 'total_rows')
            
                                                    
            download_metrics_df = spark.read.parquet(f'{ADLS_STORAGE}{DOWNLOAD_METRICS}/*.parquet')


            joined_metrics = download_metrics_df.join(F.broadcast(gkg_metrics),
                                                                on=[ download_metrics_df.gkg_record_id == gkg_metrics.gkg_record_id_a,
                                                                    download_metrics_df.translingual == gkg_metrics.translingual_a],
                                                                how='inner')
                                     
            pipeline_metrics_temp_df = joined_metrics.select('gkg_record_id', 'translingual', 'file_name', 'gkg_timestamp', 
                                                    'local_download_time', 'etl_timestamp', 'csv_size_mb', 'total_rows')

            # write PIPELINE METRICS TEMP    
            pipeline_metrics_temp_df.coalesce(1) \
                .write \
                .mode('append') \
                .format('parquet') \
                .save(f'{ADLS_STORAGE}{PIPELINE_METRICS_TEMP}') 
        
        except Exception as e:
            print(f'Encountered exception while attempting to create pipeline metrics:\n{e}')
        
    

    def write_batch(self):
        """
        """

        try:
            # DBFS_MNT prefix prepended below
            etl_out_path = self.config['ETL']['PATHS']['TRANSFORMED_OUT']

            rdd_paths = GkgBatchWriter().batch_processor()

            writer = GkgBatchWriter()
            writer.period = self.config['BATCH']['PERIOD']

            processed_gkg = GkgBatchWriter().check_processed()

            
            for value in rdd_paths:

                single_path = value.split(',')[0]

                writer.file_name = single_path.split('/')[-1]
                writer.numeric_date_str = value.split('/')[-1].split('.')[0]
                writer.date_time = datetime.datetime.strptime(writer.numeric_date_str, '%Y%m%d%H%M%S')
                writer.year = value.split('/')[-1].split('.')[0][:4]
                writer.month = value.split('/')[-1].split('.')[0][4:6]
                writer.day = value.split('/')[-1].split('.')[0][6:8]
                writer.hour = value.split('/')[-1].split('.')[0][8:10]
                writer.minute = value.split('/')[-1].split('.')[0][10:12]
                writer.second = value.split('/')[-1].split('.')[0][12:14]
                writer.gkg_version = writer.file_name[14:]

                if writer.gkg_version == '.translation.gkg.csv':
                    writer.translingual = True
                    version = 'translingual'
                else:
                    writer.translingual = False
                    version = 'eng'


                ########################################################################################
                ###########################      GKG BLOCK FROM URLS     ############################### 
                ######################################################################################## 

                gkg_paths = value[:-1]
                gkg_block_rdd = spark.sparkContext.textFile(gkg_paths)
                parsed = gkg_block_rdd.map(lambda line: gkg_parser(line))
                gkg_block_df = spark.createDataFrame(parsed, schema=gkg_schema)



                ########################################################################################
                #####################      WRITE OUT 15min / hourly mode     ########################### 
                ########################################################################################   

                if writer.period == '15min' or writer.period == 'hourly':
                    print(f'Writing ** {version.upper()} ** GKG records in {(writer.period).upper()} mode for date: {writer.year}-{writer.month}-{writer.day}')
                    gkg_block_df\
                        .coalesce(1) \
                        .write \
                        .mode('append') \
                        .format('parquet') \
                        .save(f'{ADLS_STORAGE}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/')
                    print(f'Successfully wrote ** {version.upper()} ** GKG files in {(writer.period).upper()} mode to path: {ADLS_STORAGE}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}')

                    # create pipeline metrics DF
                    GkgBatchWriter().create_pipeline_metrics(fs=ADLS_STORAGE, etl_out_path=etl_out_path, version=version, year=writer.year, month=writer.month, day=writer.day)

                

                ########################################################################################
                ##########################      WRITE OUT DAILY MODE     ############################### 
                ########################################################################################     
                
                else:
                    print(f'Writing ** {version.upper()} ** GKG records in {(writer.period).upper()} mode for date: {writer.year}-{writer.month}-{writer.day}')
                    gkg_block_df \
                            .repartition(5) \
                            .write \
                            .option('maxRecordsPerFile', 30000) \
                            .mode('append') \
                            .format('parquet') \
                            .save(f'{ADLS_STORAGE}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}/')
                    print(f'Successfully wrote ** {version.upper()} ** GKG files in {(writer.period).upper()} mode to path: {ADLS_STORAGE}{etl_out_path}/{version}/{writer.year}/{writer.month}/{writer.day}')

                    # create pipeline metrics DF
                    GkgBatchWriter().create_pipeline_metrics(fs=ADLS_STORAGE, etl_out_path=etl_out_path, version=version, year=writer.year, month=writer.month, day=writer.day)

            # for unit testing purposes
            return True

        except Exception as e:
            print(f'Encountered exception while writing batch:\n{e}')

            # for unit testing purposes
            return False



    def create_pipeline_metrics_final(self):
        """
        """
        try:
            if GkgBatchWriter().check_processed()[1] == True:
                for f in dbutils.fs.ls(f'{DBFS_MNT}{PIPELINE_METRICS_FINAL}'):
                    if not f[0].startswith('.'):
                        # full_file_name = f'{PIPELINE_METRICS_FINAL}{f}'
                        terminal_file_name = f[0].split('/')[-1]
                        cloud_file = f'{CLOUD_PATH}{PIPELINE_METRICS_FINAL}/{terminal_file_name}'

                        # dbfs:/mnt/scripts/install_requirements.sh
                        if CloudPath(cloud_file).is_file():

                            blob_file = removeprefix(str_=cloud_file, prefix=CLOUD_PATH)
                            blob_file_path = f'{DBFS_MNT}{blob_file}'
                            dbutils.fs.cp(blob_file_path, f'{DBFS_MNT}{PIPELINE_METRICS_TEMP}')


                print('joining from final pipeline metrics df')
                pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{ADLS_STORAGE}{PIPELINE_METRICS_TEMP}/*.parquet') 
                unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])
                # for DEBUG purposes - showing count including duplicates
                print(f'DEBUG: Pipeline metrics temp df count: {pipeline_metrics_temp_df.count()}')

            elif GkgBatchWriter().check_processed()[1] == False:
                print('joining from temp pipeline metrics df')
                pipeline_metrics_temp_df = spark.read.format('parquet').load(f'{ADLS_STORAGE}{PIPELINE_METRICS_TEMP}/*.parquet') 
                unique_metrics_df = pipeline_metrics_temp_df.dropDuplicates(['file_name', 'gkg_timestamp'])       
                # for DEBUG purposes - showing count including duplicates
                print(f'DEBUG: Pipeline metrics temp df count: {pipeline_metrics_temp_df.count()}')
        


            ########################################################################################
            ##################      WRITE OUT PIPELINE METRICS DF FINAL     ######################## 
            ########################################################################################     

            # first time processing GKG files the read comes from pipeline metrics temp folder (created with create_pipeline_metrics() method)
            # subsequent reads come from pipeline_metrics_final folder.

            print('Writing *** PIPELINE METRICS FINAL DF ***...')
            unique_metrics_df.coalesce(1) \
                .write \
                .mode('overwrite') \
                .format('parquet') \
                .save(f'{ADLS_STORAGE}{PIPELINE_METRICS_FINAL}')


            print('*** PIPELINE METRICS FINAL DF ***')
            pipeline_metrics_final_df = spark.read.format('parquet').load(f'{ADLS_STORAGE}{PIPELINE_METRICS_FINAL}/*.parquet')
            pipeline_metrics_final_df.sort('gkg_timestamp').show(300, truncate=False)
            print(f'Total GKG records: {pipeline_metrics_final_df.count()}')


            print(f'Deleting temp files in pipeline metrics temp folder...')
            for f in dbutils.fs.ls(f'{DBFS_MNT}{PIPELINE_METRICS_TEMP}'):
                dbutils.fs.rm(f[0])
            print(f'Deleted all files in pipeline metrics temp folder.')


        except Exception as e:
            print(f'Encountered exception while attempting to write out FINAL PIPELINE METRICS DF:\n{e}')




if __name__ == '__main__':
    
    # write out batch
    GkgBatchWriter().write_batch()

    # create pipeline metrics final
    GkgBatchWriter().create_pipeline_metrics_final()
